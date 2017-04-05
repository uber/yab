package templateargs

import (
	"github.com/yarpc/yab/templateargs/interpolate"

	"gopkg.in/yaml.v2"
)

// ProcessMap takes a YAML request that may contain values like ${name:prashant}
// and replaces any template arguments with those specified in args.
func ProcessMap(req map[interface{}]interface{}, args map[string]string) (map[interface{}]interface{}, error) {
	return processMap(req, args)
}

func processString(v string, args map[string]string) (interface{}, error) {
	parsed, err := interpolate.Parse(v)
	if err != nil {
		return nil, err
	}

	rendered, err := parsed.Render(func(name string) (value string, ok bool) {
		v, ok := args[name]
		return v, ok
	})
	if err != nil {
		return nil, err
	}

	if rendered == "" {
		return "", nil
	}

	// Otherwise, unmarshal the value and return that.
	var unmarshalled interface{}
	err = yaml.Unmarshal([]byte(rendered), &unmarshalled)
	return unmarshalled, err
}

func processValue(v interface{}, args map[string]string) (interface{}, error) {
	switch v := v.(type) {
	case string:
		return processString(v, args)
	case map[interface{}]interface{}:
		return processMap(v, args)
	case []interface{}:
		return processList(v, args)
	default:
		return v, nil
	}

}

func processList(l []interface{}, args map[string]string) ([]interface{}, error) {
	replacement := make([]interface{}, len(l))
	for i, v := range l {
		newV, err := processValue(v, args)
		if err != nil {
			return nil, err
		}
		replacement[i] = newV
	}

	return replacement, nil
}

func processMap(m map[interface{}]interface{}, args map[string]string) (map[interface{}]interface{}, error) {
	replacement := make(map[interface{}]interface{}, len(m))
	for k, v := range m {
		newK, err := processValue(k, args)
		if err != nil {
			return nil, err
		}

		newV, err := processValue(v, args)
		if err != nil {
			return nil, err
		}

		replacement[newK] = newV
	}

	return replacement, nil
}
