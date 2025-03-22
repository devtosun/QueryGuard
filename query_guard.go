package queryguard

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Operator represents SQL operators
type Operator string

const (
	Equal        Operator = "="
	NotEqual     Operator = "<>"
	Greater      Operator = ">"
	Less         Operator = "<"
	GreaterEqual Operator = ">="
	LessEqual    Operator = "<="
	Like         Operator = "LIKE"
	In           Operator = "IN"
	Between      Operator = "BETWEEN"
	IsNull       Operator = "IS NULL"
	IsNotNull    Operator = "IS NOT NULL"
	Not          Operator = "NOT"
	And          Operator = "AND"
	Or           Operator = "OR"
)

// Condition interface for all condition types
type Condition interface {
	Evaluate(entity interface{}) (bool, error)
}

// SimpleCondition represents a basic condition with field, operator, and value
type SimpleCondition struct {
	Field  string
	Op     Operator
	Value  interface{}
	Value2 interface{} // Used for BETWEEN operator
}

// CompoundCondition represents a compound condition with logical operators
type CompoundCondition struct {
	Left  Condition
	Op    Operator
	Right Condition
}

// NotCondition represents a NOT condition
type NotCondition struct {
	Condition Condition
}

// GroupCondition represents a parenthesized group of conditions
type GroupCondition struct {
	Condition Condition
}

// NewSimpleCondition creates a new simple condition
func NewSimpleCondition(field string, op Operator, value interface{}, value2 interface{}) *SimpleCondition {
	return &SimpleCondition{
		Field:  field,
		Op:     op,
		Value:  value,
		Value2: value2,
	}
}

// NewCompoundCondition creates a new compound condition
func NewCompoundCondition(left Condition, op Operator, right Condition) *CompoundCondition {
	return &CompoundCondition{
		Left:  left,
		Op:    op,
		Right: right,
	}
}

// NewNotCondition creates a new NOT condition
func NewNotCondition(condition Condition) *NotCondition {
	return &NotCondition{
		Condition: condition,
	}
}

// NewGroupCondition creates a new grouped condition
func NewGroupCondition(condition Condition) *GroupCondition {
	return &GroupCondition{
		Condition: condition,
	}
}

// Evaluate evaluates a simple condition against an entity
func (c *SimpleCondition) Evaluate(entity interface{}) (bool, error) {
	// Get the field value from the entity
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return false, fmt.Errorf("entity must be a struct or a pointer to a struct")
	}

	field := val.FieldByName(c.Field)
	if !field.IsValid() {
		return false, fmt.Errorf("field %s not found in entity", c.Field)
	}

	fieldValue := field.Interface()

	// Evaluate the condition based on the operator
	switch c.Op {
	case Equal:
		return equals(fieldValue, c.Value)
	case NotEqual:
		result, err := equals(fieldValue, c.Value)
		return !result, err
	case Greater:
		return compareValues(fieldValue, c.Value, 1)
	case Less:
		return compareValues(fieldValue, c.Value, -1)
	case GreaterEqual:
		eq, err := equals(fieldValue, c.Value)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
		return compareValues(fieldValue, c.Value, 1)
	case LessEqual:
		eq, err := equals(fieldValue, c.Value)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
		return compareValues(fieldValue, c.Value, -1)
	case Like:
		return likeOperator(fieldValue, c.Value)
	case In:
		return inOperator(fieldValue, c.Value)
	case Between:
		return betweenOperator(fieldValue, c.Value, c.Value2)
	case IsNull:
		return isNull(fieldValue)
	case IsNotNull:
		result, err := isNull(fieldValue)
		return !result, err
	default:
		return false, fmt.Errorf("unsupported operator: %s", c.Op)
	}
}

// Evaluate evaluates a compound condition against an entity
func (c *CompoundCondition) Evaluate(entity interface{}) (bool, error) {
	leftResult, err := c.Left.Evaluate(entity)
	if err != nil {
		return false, err
	}

	// Short-circuit evaluation for AND and OR
	if c.Op == And && !leftResult {
		return false, nil
	}
	if c.Op == Or && leftResult {
		return true, nil
	}

	rightResult, err := c.Right.Evaluate(entity)
	if err != nil {
		return false, err
	}

	switch c.Op {
	case And:
		return leftResult && rightResult, nil
	case Or:
		return leftResult || rightResult, nil
	default:
		return false, fmt.Errorf("unsupported compound operator: %s", c.Op)
	}
}

// Evaluate evaluates a NOT condition against an entity
func (c *NotCondition) Evaluate(entity interface{}) (bool, error) {
	result, err := c.Condition.Evaluate(entity)
	if err != nil {
		return false, err
	}
	return !result, nil
}

// Evaluate evaluates a grouped condition against an entity
func (c *GroupCondition) Evaluate(entity interface{}) (bool, error) {
	return c.Condition.Evaluate(entity)
}

// Helper functions for different operators
func equals(a, b interface{}) (bool, error) {
	// Convert values to ensure proper comparison
	aValue, bValue, err := convertToSameType(a, b)
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(aValue, bValue), nil
}

func compareValues(a, b interface{}, expectedResult int) (bool, error) {
	aValue, bValue, err := convertToSameType(a, b)
	if err != nil {
		return false, err
	}

	switch v := aValue.(type) {
	case int:
		bInt, ok := bValue.(int)
		if !ok {
			return false, fmt.Errorf("cannot compare int with non-int")
		}
		if v > bInt {
			return expectedResult > 0, nil
		} else if v < bInt {
			return expectedResult < 0, nil
		}
		return expectedResult == 0, nil
	case float64:
		bFloat, ok := bValue.(float64)
		if !ok {
			return false, fmt.Errorf("cannot compare float64 with non-float64")
		}
		if v > bFloat {
			return expectedResult > 0, nil
		} else if v < bFloat {
			return expectedResult < 0, nil
		}
		return expectedResult == 0, nil
	case string:
		bString, ok := bValue.(string)
		if !ok {
			return false, fmt.Errorf("cannot compare string with non-string")
		}
		if v > bString {
			return expectedResult > 0, nil
		} else if v < bString {
			return expectedResult < 0, nil
		}
		return expectedResult == 0, nil
	case time.Time:
		bTime, ok := bValue.(time.Time)
		if !ok {
			return false, fmt.Errorf("cannot compare time.Time with non-time.Time")
		}
		if v.After(bTime) {
			return expectedResult > 0, nil
		} else if v.Before(bTime) {
			return expectedResult < 0, nil
		}
		return expectedResult == 0, nil
	default:
		return false, fmt.Errorf("unsupported type for comparison: %T", v)
	}
}

func likeOperator(a, b interface{}) (bool, error) {
	str, ok := a.(string)
	if !ok {
		return false, fmt.Errorf("LIKE operator can only be used with string values")
	}

	pattern, ok := b.(string)
	if !ok {
		return false, fmt.Errorf("LIKE pattern must be a string")
	}

	// Convert SQL LIKE pattern to Go regex
	// This is a simplified version, might need enhancement for complex patterns
	pattern = strings.ReplaceAll(pattern, "%", ".*")
	pattern = strings.ReplaceAll(pattern, "_", ".")

	return strings.Contains(strings.ToLower(str), strings.ToLower(pattern)), nil
}

func inOperator(a, b interface{}) (bool, error) {
	// b should be a slice
	bValue := reflect.ValueOf(b)
	if bValue.Kind() != reflect.Slice && bValue.Kind() != reflect.Array {
		return false, fmt.Errorf("IN operator requires a slice or array as the second argument")
	}

	for i := 0; i < bValue.Len(); i++ {
		equal, err := equals(a, bValue.Index(i).Interface())
		if err != nil {
			return false, err
		}
		if equal {
			return true, nil
		}
	}

	return false, nil
}

func betweenOperator(a, b, c interface{}) (bool, error) {
	// Check if a is between b and c (inclusive)
	greaterThanOrEqualB, err := compareValues(a, b, 1)
	if err != nil {
		return false, err
	}
	equalB, err := equals(a, b)
	if err != nil {
		return false, err
	}

	lessThanOrEqualC, err := compareValues(a, c, -1)
	if err != nil {
		return false, err
	}
	equalC, err := equals(a, c)
	if err != nil {
		return false, err
	}

	return (greaterThanOrEqualB || equalB) && (lessThanOrEqualC || equalC), nil
}

func isNull(a interface{}) (bool, error) {
	return a == nil, nil
}

// func convertToSameType(a, b interface{}) (interface{}, interface{}, error) {
// 	// Handle nil cases
// 	if a == nil || b == nil {
// 		return a, b, nil
// 	}

// 	// Get value types
// 	aType := reflect.TypeOf(a)
// 	bType := reflect.TypeOf(b)

// 	// If types are the same, no conversion needed
// 	if aType == bType {
// 		return a, b, nil
// 	}

// 	// Handle numeric type conversions
// 	aVal := reflect.ValueOf(a)
// 	bVal := reflect.ValueOf(b)

// 	// Convert numeric types to float64 for comparison
// 	if isNumeric(aType.Kind()) && isNumeric(bType.Kind()) {
// 		aFloat := getFloat64Value(aVal)
// 		bFloat := getFloat64Value(bVal)
// 		return aFloat, bFloat, nil
// 	}

// 	// Handle time conversions
// 	if aType == reflect.TypeOf(time.Time{}) && bType.Kind() == reflect.String {
// 		// Parse the string as time
// 		timeStr := bVal.String()
// 		parsedTime, err := time.Parse(time.RFC3339, timeStr)
// 		if err != nil {
// 			// Try other common formats
// 			parsedTime, err = time.Parse("2006-01-02 15:04:05", timeStr)
// 			if err != nil {
// 				return nil, nil, fmt.Errorf("cannot convert string to time: %v", err)
// 			}
// 		}
// 		return a, parsedTime, nil
// 	}

// 	if bType == reflect.TypeOf(time.Time{}) && aType.Kind() == reflect.String {
// 		// Parse the string as time
// 		timeStr := aVal.String()
// 		parsedTime, err := time.Parse(time.RFC3339, timeStr)
// 		if err != nil {
// 			// Try other common formats
// 			parsedTime, err = time.Parse("2006-01-02 15:04:05", timeStr)
// 			if err != nil {
// 				return nil, nil, fmt.Errorf("cannot convert string to time: %v", err)
// 			}
// 		}
// 		return parsedTime, b, nil
// 	}

// 	// Handle string conversions
// 	if aType.Kind() == reflect.String || bType.Kind() == reflect.String {
// 		// Convert both to strings for comparison
// 		aStr := fmt.Sprintf("%v", a)
// 		bStr := fmt.Sprintf("%v", b)
// 		return aStr, bStr, nil
// 	}

// 	return a, b, fmt.Errorf("cannot convert between types %T and %T", a, b)
// }

func convertToSameType(a, b interface{}) (interface{}, interface{}, error) {
	// Handle nil cases
	if a == nil || b == nil {
		return a, b, nil
	}

	// Get value types
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)

	// If types are the same, no conversion needed
	if aType == bType {
		return a, b, nil
	}

	// Handle numeric type conversions
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Convert numeric types to float64 for comparison
	if isNumeric(aType.Kind()) && isNumeric(bType.Kind()) {
		aFloat := getFloat64Value(aVal)
		bFloat := getFloat64Value(bVal)
		return aFloat, bFloat, nil
	}

	// Handle bool and numeric conversion (e.g., 1 -> true, 0 -> false)
	if aType.Kind() == reflect.Bool && isNumeric(bType.Kind()) {
		aBool := aVal.Bool()
		bInt := getFloat64Value(bVal) // Convert numeric to float64 first
		if bInt != 0 && bInt != 1 {
			return nil, nil, fmt.Errorf("cannot convert numeric value %v to bool (must be 0 or 1)", b)
		}
		return aBool, bInt == 1, nil
	}
	if bType.Kind() == reflect.Bool && isNumeric(aType.Kind()) {
		bBool := bVal.Bool()
		aInt := getFloat64Value(aVal) // Convert numeric to float64 first
		if aInt != 0 && aInt != 1 {
			return nil, nil, fmt.Errorf("cannot convert numeric value %v to bool (must be 0 or 1)", a)
		}
		return aInt == 1, bBool, nil
	}

	// Handle time conversions
	if aType == reflect.TypeOf(time.Time{}) && bType.Kind() == reflect.String {
		timeStr := bVal.String()
		parsedTime, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			parsedTime, err = time.Parse("2006-01-02 15:04:05", timeStr)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot convert string to time: %v", err)
			}
		}
		return a, parsedTime, nil
	}
	if bType == reflect.TypeOf(time.Time{}) && aType.Kind() == reflect.String {
		timeStr := aVal.String()
		parsedTime, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			parsedTime, err = time.Parse("2006-01-02 15:04:05", timeStr)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot convert string to time: %v", err)
			}
		}
		return parsedTime, b, nil
	}

	// Handle string conversions
	if aType.Kind() == reflect.String || bType.Kind() == reflect.String {
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		return aStr, bStr, nil
	}

	return a, b, fmt.Errorf("cannot convert between types %T and %T", a, b)
}

func isNumeric(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func getFloat64Value(val reflect.Value) float64 {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint())
	case reflect.Float32, reflect.Float64:
		return val.Float()
	default:
		return 0
	}
}

// Helper function to build conditions programmatically for your example
func BuildExampleCondition() Condition {
	// (RetailCustomerCode = 'Mafman1' and Opendate Between GetDate() And GetDate()) OR (IsMarried = 1 And GenderCode = 0)

	// First part: (RetailCustomerCode = 'Mafman1' and Opendate Between GetDate() And GetDate())
	retailCodeCond := NewSimpleCondition("RetailCustomerCode", Equal, "Mafman1", nil)

	now := time.Now()
	openDateCond := NewSimpleCondition("OpenDate", Between, now, now)

	firstPart := NewCompoundCondition(retailCodeCond, And, openDateCond)
	firstPartGroup := NewGroupCondition(firstPart)

	// Second part: (IsMarried = 1 And GenderCode = 0)
	isMarriedCond := NewSimpleCondition("IsMarried", Equal, true, nil)
	genderCodeCond := NewSimpleCondition("GenderCode", Equal, 0, nil)

	secondPart := NewCompoundCondition(isMarriedCond, And, genderCodeCond)
	secondPartGroup := NewGroupCondition(secondPart)

	// Combine with OR
	return NewCompoundCondition(firstPartGroup, Or, secondPartGroup)
}

// Generic function to evaluate condition against any struct
func EvaluateCondition[T any](entity T, condition Condition) (bool, error) {
	return condition.Evaluate(entity)
}

// Parse parses a SQL WHERE condition string into a Condition object
func Parse(conditionStr string) (Condition, error) {
	// Trim any extra whitespace
	conditionStr = strings.TrimSpace(conditionStr)
	if conditionStr == "" {
		return nil, fmt.Errorf("empty condition string")
	}

	// Create a parser state
	parser := &conditionParser{
		input: conditionStr,
		pos:   0,
	}

	// Parse the condition
	return parser.parseCondition()
}

// conditionParser holds the state for parsing
type conditionParser struct {
	input string
	pos   int
}

// parseCondition parses a condition expression (top level)
func (p *conditionParser) parseCondition() (Condition, error) {
	// Parse the first operand (could be a simple condition or a group)
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	// Check if we're at the end of the input
	if p.pos >= len(p.input) {
		return left, nil
	}

	// Look for logical operators (AND/OR)
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return left, nil
	}

	// Check for logical operators
	if p.matchKeyword("AND") || p.matchKeyword("OR") {
		op := p.parseOperator()
		right, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		return NewCompoundCondition(left, op, right), nil
	}

	return left, nil
}

// parseOperand parses an operand (simple condition or group)
func (p *conditionParser) parseOperand() (Condition, error) {
	p.skipWhitespace()

	// Check for NOT operator
	if p.matchKeyword("NOT") {
		// Skip the NOT keyword
		p.pos += 3
		p.skipWhitespace()

		// Parse the condition being negated
		condition, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return NewNotCondition(condition), nil
	}

	// Check for opening parenthesis (grouped condition)
	if p.pos < len(p.input) && p.input[p.pos] == '(' {
		p.pos++ // Skip the opening parenthesis

		// Parse the condition inside the parentheses
		condition, err := p.parseCondition()
		if err != nil {
			return nil, err
		}

		// Skip any whitespace
		p.skipWhitespace()

		// Expect a closing parenthesis
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return nil, fmt.Errorf("expected closing parenthesis at position %d", p.pos)
		}
		p.pos++ // Skip the closing parenthesis

		return NewGroupCondition(condition), nil
	}

	// Otherwise, it's a simple condition
	return p.parseSimpleCondition()
}

// parseSimpleCondition parses a simple condition (field operator value)
func (p *conditionParser) parseSimpleCondition() (Condition, error) {
	// Parse the field name
	p.skipWhitespace()
	fieldName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	// Parse the operator
	p.skipWhitespace()
	operator, err := p.parseSimpleOperator()
	if err != nil {
		return nil, err
	}

	// Special handling for IS NULL and IS NOT NULL
	if operator == IsNull || operator == IsNotNull {
		return NewSimpleCondition(fieldName, operator, nil, nil), nil
	}

	// Parse the value(s)
	p.skipWhitespace()

	// Special handling for BETWEEN operator
	if operator == Between {
		// Parse first value
		value1, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		// Expect AND keyword
		p.skipWhitespace()
		if !p.matchKeyword("AND") {
			return nil, fmt.Errorf("expected AND keyword after first value for BETWEEN operator")
		}
		p.pos += 3 // Skip "AND"

		// Parse second value
		p.skipWhitespace()
		value2, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		return NewSimpleCondition(fieldName, operator, value1, value2), nil
	}

	// Special handling for IN operator
	if operator == In {
		// Expect opening parenthesis
		if p.pos >= len(p.input) || p.input[p.pos] != '(' {
			return nil, fmt.Errorf("expected opening parenthesis after IN operator")
		}
		p.pos++ // Skip '('

		// Parse the list of values
		values, err := p.parseValueList()
		if err != nil {
			return nil, err
		}

		// Expect closing parenthesis
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return nil, fmt.Errorf("expected closing parenthesis after IN values")
		}
		p.pos++ // Skip ')'

		return NewSimpleCondition(fieldName, operator, values, nil), nil
	}

	// For all other operators, parse a single value
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	return NewSimpleCondition(fieldName, operator, value, nil), nil
}

// parseIdentifier parses a field name or identifier
func (p *conditionParser) parseIdentifier() (string, error) {
	p.skipWhitespace()

	// Check if we have a valid identifier start
	if p.pos >= len(p.input) || !isValidIdentifierStart(p.input[p.pos]) {
		return "", fmt.Errorf("expected identifier at position %d", p.pos)
	}

	start := p.pos
	for p.pos < len(p.input) && isValidIdentifierChar(p.input[p.pos]) {
		p.pos++
	}

	return p.input[start:p.pos], nil
}

// parseOperator parses a logical operator (AND/OR)
func (p *conditionParser) parseOperator() Operator {
	p.skipWhitespace()

	if p.matchKeyword("AND") {
		p.pos += 3 // Skip "AND"
		return And
	}

	if p.matchKeyword("OR") {
		p.pos += 2 // Skip "OR"
		return Or
	}

	return ""
}

// parseSimpleOperator parses a comparison operator
func (p *conditionParser) parseSimpleOperator() (Operator, error) {
	p.skipWhitespace()

	// Check for special keywords
	if p.matchKeyword("BETWEEN") {
		p.pos += 7 // Skip "BETWEEN"
		return Between, nil
	}

	if p.matchKeyword("LIKE") {
		p.pos += 4 // Skip "LIKE"
		return Like, nil
	}

	if p.matchKeyword("IN") {
		p.pos += 2 // Skip "IN"
		return In, nil
	}

	if p.matchKeyword("IS NULL") {
		p.pos += 7 // Skip "IS NULL"
		return IsNull, nil
	}

	if p.matchKeyword("IS NOT NULL") {
		p.pos += 11 // Skip "IS NOT NULL"
		return IsNotNull, nil
	}

	// Check for symbolic operators
	if p.pos < len(p.input) {
		switch p.input[p.pos] {
		case '=':
			p.pos++
			return Equal, nil
		case '>':
			p.pos++
			if p.pos < len(p.input) && p.input[p.pos] == '=' {
				p.pos++
				return GreaterEqual, nil
			}
			return Greater, nil
		case '<':
			p.pos++
			if p.pos < len(p.input) && p.input[p.pos] == '=' {
				p.pos++
				return LessEqual, nil
			}
			if p.pos < len(p.input) && p.input[p.pos] == '>' {
				p.pos++
				return NotEqual, nil
			}
			return Less, nil
		}
	}

	return "", fmt.Errorf("unknown operator at position %d", p.pos)
}

// parseValue parses a single value (string, number, boolean, or function)
func (p *conditionParser) parseValue() (interface{}, error) {
	p.skipWhitespace()

	// Check for end of input
	if p.pos >= len(p.input) {
		return nil, fmt.Errorf("unexpected end of input while parsing value")
	}

	// Check for string literals
	if p.input[p.pos] == '\'' || p.input[p.pos] == '"' {
		return p.parseStringLiteral()
	}

	// Check for numeric literals
	if isDigit(p.input[p.pos]) || (p.input[p.pos] == '-' && p.pos+1 < len(p.input) && isDigit(p.input[p.pos+1])) {
		return p.parseNumericLiteral()
	}

	// Check for boolean literals
	if p.matchKeyword("TRUE") || p.matchKeyword("1") {
		if p.matchKeyword("TRUE") {
			p.pos += 4 // Skip "TRUE"
		} else {
			p.pos += 1 // Skip "1"
		}
		return true, nil
	}

	if p.matchKeyword("FALSE") || p.matchKeyword("0") {
		if p.matchKeyword("FALSE") {
			p.pos += 5 // Skip "FALSE"
		} else {
			p.pos += 1 // Skip "0"
		}
		return false, nil
	}

	// Check for functions (like GetDate())
	if p.matchKeyword("GetDate()") {
		p.pos += 9 // Skip "GetDate()"
		return time.Now(), nil
	}

	// If it's not a recognized literal, try to parse as an identifier
	identifier, err := p.parseIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected value at position %d", p.pos)
	}

	// This could be a parameter or a column reference
	// For now, we'll just return it as a string
	return identifier, nil
}

// parseStringLiteral parses a string literal (enclosed in quotes)
func (p *conditionParser) parseStringLiteral() (string, error) {
	// Remember the quote character
	quote := p.input[p.pos]
	p.pos++

	start := p.pos
	escaped := false
	var result strings.Builder

	for p.pos < len(p.input) {
		ch := p.input[p.pos]

		if escaped {
			// Handle escaped character
			result.WriteByte(ch)
			escaped = false
		} else if ch == '\\' {
			// Start of escape sequence
			escaped = true
		} else if ch == quote {
			// End of string
			p.pos++
			return result.String(), nil
		} else {
			// Regular character
			result.WriteByte(ch)
		}

		p.pos++
	}

	return "", fmt.Errorf("unterminated string literal starting at position %d", start-1)
}

// parseNumericLiteral parses a numeric literal
func (p *conditionParser) parseNumericLiteral() (interface{}, error) {
	start := p.pos
	hasDecimal := false

	// Handle negative numbers
	if p.input[p.pos] == '-' {
		p.pos++
	}

	// Parse digits
	for p.pos < len(p.input) && (isDigit(p.input[p.pos]) || p.input[p.pos] == '.') {
		if p.input[p.pos] == '.' {
			if hasDecimal {
				return nil, fmt.Errorf("invalid numeric literal: multiple decimal points")
			}
			hasDecimal = true
		}
		p.pos++
	}

	// Convert to appropriate numeric type
	numStr := p.input[start:p.pos]
	if hasDecimal {
		val, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float literal: %s", numStr)
		}
		return val, nil
	} else {
		val, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer literal: %s", numStr)
		}

		// Return as int if it fits
		if val >= int64(^uint(0)>>1) {
			return int(val), nil
		}
		return val, nil
	}
}

// parseValueList parses a comma-separated list of values
func (p *conditionParser) parseValueList() ([]interface{}, error) {
	var values []interface{}

	for {
		p.skipWhitespace()

		// Parse a value
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		values = append(values, value)

		// Check for comma
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.input[p.pos] != ',' {
			break
		}
		p.pos++ // Skip comma
	}

	return values, nil
}

// Helper functions
func (p *conditionParser) skipWhitespace() {
	for p.pos < len(p.input) && isWhitespace(p.input[p.pos]) {
		p.pos++
	}
}

func (p *conditionParser) matchKeyword(keyword string) bool {
	if p.pos+len(keyword) <= len(p.input) {
		// Get the substring to check
		substr := p.input[p.pos : p.pos+len(keyword)]

		// Case-insensitive comparison
		if strings.EqualFold(substr, keyword) {
			// Make sure it's a complete keyword (followed by whitespace, operator, or end of input)
			nextPos := p.pos + len(keyword)
			if nextPos >= len(p.input) ||
				isWhitespace(p.input[nextPos]) ||
				isOperatorChar(p.input[nextPos]) ||
				p.input[nextPos] == '(' ||
				p.input[nextPos] == ')' ||
				p.input[nextPos] == ',' {
				return true
			}
		}
	}
	return false
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isValidIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isValidIdentifierChar(ch byte) bool {
	return isValidIdentifierStart(ch) || isDigit(ch)
}

func isOperatorChar(ch byte) bool {
	return ch == '=' || ch == '<' || ch == '>' || ch == '!' || ch == '+' || ch == '-' || ch == '*' || ch == '/'
}

// Helper function to create a condition from a SQL WHERE clause string
func CreateConditionFromSQL(sqlWhere string) (Condition, error) {
	return Parse(sqlWhere)
}
