package clients

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
)

type TLS struct {
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify,omitempty" json:"insecureSkipVerify,omitempty" doc:"skip client verifying the server's certificate chain and host name"`
	CACertPath         string `yaml:"caCertPath,omitempty" json:"caCertPath,omitempty" doc:"path to the CA certificate"`
	UserCertPath       string `yaml:"userCertPath,omitempty" json:"userCertPath,omitempty" doc:"path to the user certificate"`
	UserKeyPath        string `yaml:"userKeyPath,omitempty" json:"userKeyPath,omitempty" doc:"path to the user private key"`
}

func (c *TLS) Build() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.InsecureSkipVerify,
	}
	if c.CACertPath != "" {
		caCert, err := ioutil.ReadFile(c.CACertPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(caCert)

		if c.UserCertPath != "" && c.UserKeyPath != "" {
			userCert, err := ioutil.ReadFile(c.UserCertPath)
			if err != nil {
				return nil, err
			}
			userKey, err := ioutil.ReadFile(c.UserKeyPath)
			if err != nil {
				return nil, err
			}
			pair, err := tls.X509KeyPair([]byte(userCert), []byte(userKey))
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{pair}
		} else if c.UserCertPath != "" || c.UserKeyPath != "" {
			return nil, errors.New("userCertPath and userKeyPath must be both present or both absent.")
		}
		return tlsConfig, nil
	}
	return nil, nil
}
