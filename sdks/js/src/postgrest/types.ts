import type { BasinError } from "../errors.js";

export type CountOption = "exact" | "planned" | "estimated";

export interface PostgrestResponse<T> {
  data: T[] | null;
  error: BasinError | null;
  count: number | null;
  status: number;
}

export interface PostgrestSingleResponse<T> {
  data: T | null;
  error: BasinError | null;
  status: number;
}
