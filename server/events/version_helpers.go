package events

import version "github.com/hashicorp/go-version"

// terraformVersionString converts version.Version to string
func terraformVersionString(v *version.Version) string {
	if v == nil {
		return ""
	}
	return v.String()
}
