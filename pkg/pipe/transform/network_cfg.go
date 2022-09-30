package transform

type NetworkConfig struct {
	Rules          NetworkTransformRules `yaml:"rules" json:"rules" doc:"list of transform rules, each includes:"`
	KubeConfigPath string                `yaml:"kubeConfigPath,omitempty" json:"kubeConfigPath,omitempty" doc:"path to kubeconfig file (optional)"`
	ServicesFile   string                `yaml:"servicesFile,omitempty" json:"servicesFile,omitempty" doc:"path to services file (optional, default: /etc/services)"`
	ProtocolsFile  string                `yaml:"protocolsFile,omitempty" json:"protocolsFile,omitempty" doc:"path to protocols file (optional, default: /etc/protocols)"`
}

func (tn *NetworkConfig) GetServiceFiles() (string, string) {
	p := tn.ProtocolsFile
	if p == "" {
		p = "/etc/protocols"
	}
	s := tn.ServicesFile
	if s == "" {
		s = "/etc/services"
	}
	return p, s
}

type TransformNetworkOperationEnum struct {
	AddRegExIf    string `yaml:"add_regex_if" json:"add_regex_if" doc:"add output field if input field satisfies regex pattern from parameters field"`
	AddIf         string `yaml:"add_if" json:"add_if" doc:"add output field if input field satisfies criteria from parameters field"`
	AddSubnet     string `yaml:"add_subnet" json:"add_subnet" doc:"add output subnet field from input field and prefix length from parameters field"`
	AddLocation   string `yaml:"add_location" json:"add_location" doc:"add output location fields from input"`
	AddService    string `yaml:"add_service" json:"add_service" doc:"add output network service field from input port and parameters protocol field"`
	AddKubernetes string `yaml:"add_kubernetes" json:"add_kubernetes" doc:"add output kubernetes fields from input"`
}

type NetworkTransformRule struct {
	Input      string `yaml:"input,omitempty" json:"input,omitempty" doc:"entry input field"`
	Output     string `yaml:"output,omitempty" json:"output,omitempty" doc:"entry output field"`
	Type       string `yaml:"type,omitempty" json:"type,omitempty" enum:"TransformNetworkOperationEnum" doc:"one of the following:"`
	Parameters string `yaml:"parameters,omitempty" json:"parameters,omitempty" doc:"parameters specific to type"`
	Assignee   string `yaml:"assignee,omitempty" json:"assignee,omitempty" doc:"value needs to assign to output field"`
}

type NetworkTransformRules []NetworkTransformRule
