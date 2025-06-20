package archiver

import (
	"net/url"
)

type (
	// URI identifies the archival resource to which records are written to and read from.
	URI interface {
		Scheme() string
		Path() string
		Hostname() string
		Port() string
		Username() string
		Password() string
		String() string
		Opaque() string
		Query() map[string][]string
	}

	uri struct {
		url *url.URL
	}
)

// NewURI constructs a new archiver URI from string.
func NewURI(s string) (URI, error) {
	url, err := url.ParseRequestURI(s)
	if err != nil {
		return nil, err
	}
	return &uri{url: url}, nil
}

func (u *uri) Scheme() string {
	return u.url.Scheme
}

func (u *uri) Path() string {
	return u.url.Path
}

func (u *uri) Hostname() string {
	return u.url.Hostname()
}

func (u *uri) Port() string {
	return u.url.Port()
}

func (u *uri) Username() string {
	if u.url.User == nil {
		return ""
	}
	return u.url.User.Username()
}

func (u *uri) Password() string {
	if u.url.User == nil {
		return ""
	}
	password, exist := u.url.User.Password()
	if !exist {
		return ""
	}
	return password
}

func (u *uri) Opaque() string {
	return u.url.Opaque
}

func (u *uri) Query() map[string][]string {
	return u.url.Query()
}

func (u *uri) String() string {
	return u.url.String()
}
