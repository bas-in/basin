// Live GORM suite — AutoMigrate (GORM's own migrator + information_schema
// introspection), CRUD with RETURNING, Preload, CreateInBatches, clause-based
// upsert, and the transaction manager, against BASIN_DSN.
//
// TAP-ish output: "ok - gorm.<test>" / "not ok - gorm.<test> # <reason>".
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type GUser struct {
	ID        uint   `gorm:"primaryKey"`
	Email     string `gorm:"uniqueIndex;not null"`
	Name      string
	CreatedAt time.Time
	Posts     []GPost `gorm:"foreignKey:AuthorID"`
}

type GPost struct {
	ID       uint `gorm:"primaryKey"`
	Title    string
	Views    int
	AuthorID uint
}

// GAccount exercises the long-tail column kinds: a soft-delete column
// (gorm.DeletedAt -> the `deleted_at IS NULL` injection + UPDATE-on-Delete),
// an optimistic-lock version counter, and a balance for arithmetic UPDATEs.
type GAccount struct {
	ID        uint `gorm:"primaryKey"`
	Owner     string
	Balance   int64
	Version   int64
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

var run = time.Now().UnixNano()

func ok(name string) { fmt.Printf("ok - gorm.%s\n", name) }

func notok(name string, err error) {
	reason := "unknown"
	if err != nil {
		reason = strings.Join(strings.Fields(err.Error()), " ")
		if len(reason) > 300 {
			reason = reason[:300]
		}
	}
	fmt.Printf("not ok - gorm.%s # %s\n", name, reason)
}

func test(name string, fn func() error) {
	if err := fn(); err != nil {
		notok(name, err)
	} else {
		ok(name)
	}
}

func check(cond bool, msg string) error {
	if !cond {
		return fmt.Errorf("assertion failed: %s", msg)
	}
	return nil
}

func main() {
	dsn := os.Getenv("BASIN_DSN")
	remaining := []string{"automigrate", "crud-create", "crud-read-preload",
		"batch-create", "upsert", "transaction", "transaction-rollback", "delete",
		"group-by", "distinct", "count-where", "locking-for-update",
		"locking-skip-locked", "soft-delete", "optimistic-lock", "create-in-batches"}
	if dsn == "" {
		notok("connect", errors.New("BASIN_DSN not set"))
		return
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err == nil {
		sqlDB, derr := db.DB()
		if derr == nil {
			err = sqlDB.Ping()
		} else {
			err = derr
		}
	}
	if err != nil {
		notok("connect", err)
		for _, t := range remaining {
			notok(t, errors.New("SKIP-CASCADE: no connection"))
		}
		return
	}
	ok("connect")

	test("automigrate", func() error {
		return db.AutoMigrate(&GUser{}, &GPost{}, &GAccount{})
	})

	email := fmt.Sprintf("alice.%d@gorm.test", run)
	var alice GUser

	test("crud-create", func() error {
		alice = GUser{Email: email, Name: "Alice", Posts: []GPost{
			{Title: "g-one", Views: 5},
			{Title: "g-two"},
		}}
		if err := db.Create(&alice).Error; err != nil {
			return err
		}
		if err := check(alice.ID > 0, "Create hydrated the primary key (RETURNING)"); err != nil {
			return err
		}
		return check(alice.Posts[0].ID > 0, "associated posts created with ids")
	})

	test("crud-read-preload", func() error {
		var got GUser
		if err := db.Preload("Posts").Where("email = ?", email).First(&got).Error; err != nil {
			return err
		}
		if err := check(got.Name == "Alice", "First found the row"); err != nil {
			return err
		}
		return check(len(got.Posts) == 2, "Preload hydrated both children")
	})

	test("batch-create", func() error {
		users := make([]GUser, 0, 10)
		for i := 0; i < 10; i++ {
			users = append(users, GUser{Email: fmt.Sprintf("batch.%d.%d@gorm.test", run, i)})
		}
		if err := db.CreateInBatches(users, 3).Error; err != nil {
			return err
		}
		var n int64
		if err := db.Model(&GUser{}).
			Where("email LIKE ?", fmt.Sprintf("batch.%d.%%", run)).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 10, fmt.Sprintf("all 10 batch rows persisted (got %d)", n))
	})

	test("upsert", func() error {
		u := GUser{Email: email, Name: "Alice Upserted"}
		if err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "email"}},
			DoUpdates: clause.AssignmentColumns([]string{"name"}),
		}).Create(&u).Error; err != nil {
			return err
		}
		var got GUser
		if err := db.Where("email = ?", email).First(&got).Error; err != nil {
			return err
		}
		if err := check(got.Name == "Alice Upserted", "DO UPDATE applied excluded.name"); err != nil {
			return err
		}
		var n int64
		if err := db.Model(&GUser{}).Where("email = ?", email).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 1, "upsert did not duplicate the row")
	})

	txEmail := fmt.Sprintf("tx.%d@gorm.test", run)
	test("transaction", func() error {
		if err := db.Transaction(func(tx *gorm.DB) error {
			u := GUser{Email: txEmail}
			if err := tx.Create(&u).Error; err != nil {
				return err
			}
			return tx.Create(&GPost{Title: "in-tx", AuthorID: u.ID}).Error
		}); err != nil {
			return err
		}
		var n int64
		if err := db.Model(&GUser{}).Where("email = ?", txEmail).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 1, "committed transaction visible")
	})

	test("transaction-rollback", func() error {
		rbEmail := fmt.Sprintf("rb.%d@gorm.test", run)
		_ = db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&GUser{Email: rbEmail}).Error; err != nil {
				return err
			}
			return errors.New("force rollback")
		})
		var n int64
		if err := db.Model(&GUser{}).Where("email = ?", rbEmail).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 0, "rolled-back insert must not be visible")
	})

	test("delete", func() error {
		res := db.Where("title = ?", "in-tx").Delete(&GPost{})
		if res.Error != nil {
			return res.Error
		}
		return check(res.RowsAffected >= 1, "delete reported affected rows")
	})

	// ── Aggregations / groupBy ──────────────────────────────────────────────
	test("group-by", func() error {
		type row struct {
			AuthorID uint
			N        int64
			S        int64
		}
		var rows []row
		if err := db.Model(&GPost{}).
			Select("author_id, count(*) as n, sum(views) as s").
			Group("author_id").
			Order("author_id").
			Scan(&rows).Error; err != nil {
			return err
		}
		return check(len(rows) >= 1, "Group(author_id) returned grouped rows")
	})

	test("distinct", func() error {
		var roles []uint
		if err := db.Model(&GPost{}).Distinct("author_id").Pluck("author_id", &roles).Error; err != nil {
			return err
		}
		return check(len(roles) >= 1, "Distinct(author_id) returned distinct values")
	})

	test("count-where", func() error {
		var n int64
		if err := db.Model(&GUser{}).Where("email = ?", email).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 1, "Count() with WHERE returned 1")
	})

	// ── Row-level locking (clause.Locking) ──────────────────────────────────
	test("locking-for-update", func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var u GUser
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("email = ?", email).First(&u).Error; err != nil {
				return err
			}
			return check(u.ID > 0, "FOR UPDATE locked-read returned the row")
		})
	})

	test("locking-skip-locked", func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var posts []GPost
			err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Limit(1).Find(&posts).Error
			if err != nil {
				return err
			}
			return check(true, "FOR UPDATE SKIP LOCKED executed")
		})
	})

	// ── Soft delete (gorm.DeletedAt) ────────────────────────────────────────
	test("soft-delete", func() error {
		acct := GAccount{Owner: fmt.Sprintf("sd.%d", run), Balance: 100}
		if err := db.Create(&acct).Error; err != nil {
			return err
		}
		// Delete() on a model with gorm.DeletedAt is a soft delete (UPDATE
		// deleted_at = now()); the row stays in storage but is filtered out.
		if err := db.Delete(&acct).Error; err != nil {
			return err
		}
		var n int64
		if err := db.Model(&GAccount{}).Where("id = ?", acct.ID).Count(&n).Error; err != nil {
			return err
		}
		if err := check(n == 0, "soft-deleted row excluded by the deleted_at IS NULL filter"); err != nil {
			return err
		}
		// Unscoped() bypasses the soft-delete filter — the row is still there.
		var m int64
		if err := db.Unscoped().Model(&GAccount{}).Where("id = ?", acct.ID).Count(&m).Error; err != nil {
			return err
		}
		return check(m == 1, "Unscoped() sees the soft-deleted row")
	})

	// ── Optimistic locking (version-guarded UPDATE) ─────────────────────────
	test("optimistic-lock", func() error {
		acct := GAccount{Owner: fmt.Sprintf("ol.%d", run), Balance: 10, Version: 0}
		if err := db.Create(&acct).Error; err != nil {
			return err
		}
		// UPDATE ... WHERE id = ? AND version = ? (the classic guard).
		res := db.Model(&GAccount{}).
			Where("id = ? AND version = ?", acct.ID, 0).
			Updates(map[string]interface{}{"balance": 20, "version": gorm.Expr("version + 1")})
		if res.Error != nil {
			return res.Error
		}
		return check(res.RowsAffected == 1, "version-guarded UPDATE affected exactly one row")
	})

	// ── CreateInBatches over the account table ──────────────────────────────
	test("create-in-batches", func() error {
		accts := make([]GAccount, 0, 7)
		for i := 0; i < 7; i++ {
			accts = append(accts, GAccount{Owner: fmt.Sprintf("cib.%d.%d", run, i), Balance: int64(i)})
		}
		if err := db.CreateInBatches(accts, 3).Error; err != nil {
			return err
		}
		var n int64
		if err := db.Model(&GAccount{}).
			Where("owner LIKE ?", fmt.Sprintf("cib.%d.%%", run)).Count(&n).Error; err != nil {
			return err
		}
		return check(n == 7, fmt.Sprintf("CreateInBatches persisted all 7 rows (got %d)", n))
	})
}
