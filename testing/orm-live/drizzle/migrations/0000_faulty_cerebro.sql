CREATE TABLE "d_posts" (
	"id" serial PRIMARY KEY NOT NULL,
	"title" text NOT NULL,
	"published" boolean DEFAULT false NOT NULL,
	"views" integer DEFAULT 0 NOT NULL,
	"author_id" integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE "d_users" (
	"id" serial PRIMARY KEY NOT NULL,
	"email" text NOT NULL,
	"name" text,
	"meta" jsonb,
	"created_at" timestamp with time zone DEFAULT now(),
	CONSTRAINT "d_users_email_unique" UNIQUE("email")
);
--> statement-breakpoint
ALTER TABLE "d_posts" ADD CONSTRAINT "d_posts_author_id_d_users_id_fk" FOREIGN KEY ("author_id") REFERENCES "public"."d_users"("id") ON DELETE no action ON UPDATE no action;