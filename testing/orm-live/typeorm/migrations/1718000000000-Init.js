// TypeORM migration — applied through TypeORM's own migration runner
// (dataSource.runMigrations()), which bootstraps the "migrations" tracker
// table exactly like a real app deploy.
export class Init1718000000000 {
  name = "Init1718000000000";

  async up(queryRunner) {
    await queryRunner.query(
      `CREATE TABLE "t_users" (
         "id" SERIAL NOT NULL,
         "email" character varying NOT NULL,
         "name" character varying,
         "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
         CONSTRAINT "UQ_t_users_email" UNIQUE ("email"),
         CONSTRAINT "PK_t_users" PRIMARY KEY ("id")
       )`,
    );
    await queryRunner.query(
      `CREATE TABLE "t_posts" (
         "id" SERIAL NOT NULL,
         "title" character varying NOT NULL,
         "views" integer NOT NULL DEFAULT '0',
         "author_id" integer NOT NULL,
         CONSTRAINT "PK_t_posts" PRIMARY KEY ("id")
       )`,
    );
    await queryRunner.query(
      `ALTER TABLE "t_posts" ADD CONSTRAINT "FK_t_posts_author"
         FOREIGN KEY ("author_id") REFERENCES "t_users"("id")
         ON DELETE NO ACTION ON UPDATE NO ACTION`,
    );
  }

  async down(queryRunner) {
    await queryRunner.query(`ALTER TABLE "t_posts" DROP CONSTRAINT "FK_t_posts_author"`);
    await queryRunner.query(`DROP TABLE "t_posts"`);
    await queryRunner.query(`DROP TABLE "t_users"`);
  }
}
