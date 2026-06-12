// TypeORM migration 0002 — the ALTER-heavy second migration. Exercises the
// breadth of ALTER variants TypeORM's QueryRunner emits after the initial
// CreateTable: ADD COLUMN (x2), CREATE INDEX, RENAME COLUMN (round-trip), and
// ALTER COLUMN TYPE. Applied through TypeORM's own migration runner.
export class AlterBreadth1718000001000 {
  name = "AlterBreadth1718000001000";

  async up(queryRunner) {
    // ADD COLUMN — optimistic-lock version counter (@VersionColumn).
    await queryRunner.query(`ALTER TABLE "t_users" ADD "version" integer NOT NULL DEFAULT 1`);
    // ADD COLUMN — soft-delete column (@DeleteDateColumn).
    await queryRunner.query(`ALTER TABLE "t_users" ADD "deleted_at" TIMESTAMP WITH TIME ZONE`);
    // CREATE INDEX — index-add breadth.
    await queryRunner.query(`CREATE INDEX "IDX_t_users_name" ON "t_users" ("name")`);
    // RENAME COLUMN — round-trip so the entity column name still resolves.
    await queryRunner.query(`ALTER TABLE "t_posts" RENAME COLUMN "views" TO "view_count"`);
    await queryRunner.query(`ALTER TABLE "t_posts" RENAME COLUMN "view_count" TO "views"`);
    // ALTER COLUMN TYPE — widen the title column.
    await queryRunner.query(`ALTER TABLE "t_posts" ALTER COLUMN "title" TYPE character varying(255)`);
  }

  async down(queryRunner) {
    await queryRunner.query(`DROP INDEX "IDX_t_users_name"`);
    await queryRunner.query(`ALTER TABLE "t_users" DROP COLUMN "deleted_at"`);
    await queryRunner.query(`ALTER TABLE "t_users" DROP COLUMN "version"`);
  }
}
