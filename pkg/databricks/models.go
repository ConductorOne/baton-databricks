package databricks

type User struct {
	ID     string `json:"id"`
	Emails []struct {
		Primary bool   `json:"primary"`
		Value   string `json:"value"`
	} `json:"emails"`
	UserName    string `json:"userName"`
	DisplayName string `json:"displayName"`
	Active      bool   `json:"active"`
}
