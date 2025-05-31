package validation

import (
	"encoding/json"
	"fmt"
	"strings" // Import strings for strings.NewReader

	"github.com/santhosh-tekuri/jsonschema/v5"
	_ "github.com/santhosh-tekuri/jsonschema/v5/httploader"
)

// ValidateJSONWithSchema validates a JSON data string against a JSON schema string.
func ValidateJSONWithSchema(schemaJSON string, dataJSON string) error {
	if schemaJSON == "" {
		return nil
	}

	compiler := jsonschema.NewCompiler()
	// Use AddResource and Compile for schema from string
	if err := compiler.AddResource("schema.json", strings.NewReader(schemaJSON)); err != nil {
		return fmt.Errorf("failed to add schema resource: %w", err)
	}
	sch, err := compiler.Compile("schema.json")
	if err != nil {
		return fmt.Errorf("failed to compile JSON schema: %w. Schema: %s", err, schemaJSON)
	}

	var data interface{}
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return fmt.Errorf("failed to unmarshal JSON data: %w. Data: %s", err, dataJSON)
	}

	if err := sch.Validate(data); err != nil {
		validationErr, ok := err.(*jsonschema.ValidationError)
		if ok {
			return fmt.Errorf("JSON data failed validation against schema: %v", validationErr)
		}
		return fmt.Errorf("JSON data failed validation (unexpected error type): %w", err)
	}
	return nil
}
