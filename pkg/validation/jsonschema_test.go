package validation

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestValidateJSONWithSchema_Valid(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": { "name": {"type": "string"}, "age": {"type": "integer"} },
		"required": ["name"]
	}`
	validData := `{"name": "John Doe", "age": 30}`
	assert.NoError(t, ValidateJSONWithSchema(schema, validData))
	validDataOnlyName := `{"name": "Jane Doe"}`
	assert.NoError(t, ValidateJSONWithSchema(schema, validDataOnlyName))
}

func TestValidateJSONWithSchema_Invalid(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": { "name": {"type": "string"}, "age": {"type": "integer", "minimum": 0} },
		"required": ["name", "age"]
	}`
	missingRequiredField := `{"name": "Test"}`
	err := ValidateJSONWithSchema(schema, missingRequiredField)
	assert.Error(t, err)
	if err != nil { assert.Contains(t, err.Error(), "missing properties: 'age'") } // Adjusted assertion

	wrongType := `{"name": "Test", "age": "thirty"}`
	err = ValidateJSONWithSchema(schema, wrongType)
	assert.Error(t, err)
	if err != nil { assert.Contains(t, err.Error(), "expected integer, but got string") }

	violatesMinimum := `{"name": "Test", "age": -5}`
	err = ValidateJSONWithSchema(schema, violatesMinimum)
	assert.Error(t, err)
	if err != nil { assert.Contains(t, err.Error(), "must be >= 0 but found -5") } // Adjusted assertion
}

func TestValidateJSONWithSchema_EmptySchema(t *testing.T) {
	assert.NoError(t, ValidateJSONWithSchema("", `{"name": "Test"}`))
}

func TestValidateJSONWithSchema_InvalidSchema(t *testing.T) {
	err := ValidateJSONWithSchema(`{"type": "object", "properties": {"name": {"type": "str"}}}`, `{"name": "Test"}`)
	assert.Error(t, err)
	if err != nil { assert.Contains(t, err.Error(), "failed to compile JSON schema") }
}

func TestValidateJSONWithSchema_EmptyData(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}`
	err := ValidateJSONWithSchema(schema, `{}`)
	assert.Error(t, err)
	if err != nil { assert.Contains(t, err.Error(), "missing properties: 'name'") } // Adjusted assertion

	err = ValidateJSONWithSchema(schema, "")
	assert.Error(t, err)
    if err != nil { assert.Contains(t, err.Error(), "failed to unmarshal JSON data") } // Adjusted for empty string unmarshal error
}
// internalHelperContains removed as direct strings.Contains is fine in tests.
