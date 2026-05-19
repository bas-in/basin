export interface Credential {
  id: string;
  project_id: string;
  pgwire_user: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

export interface ProvisionResult {
  connectionString: string;
}
