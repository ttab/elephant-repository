package repository

type TokenResponse struct {
	AccessToken  string `json:"access_token"` //nolint:gosec // JSON field name, not a credential.
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"` //nolint:gosec // JSON field name, not a credential.
}
