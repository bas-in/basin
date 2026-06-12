import "reflect-metadata";
import { DataSource } from "typeorm";
import { User, Post } from "./entities.js";
import { Init1718000000000 } from "./migrations/1718000000000-Init.js";

export const dataSource = new DataSource({
  type: "postgres",
  url: process.env.BASIN_DSN,
  ssl: false,
  entities: [User, Post],
  migrations: [Init1718000000000],
  synchronize: false,
  logging: false,
  connectTimeoutMS: 10_000,
});
