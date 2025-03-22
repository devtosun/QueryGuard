package queryguard

import (
	"fmt"
	"testing"
	"time"
)

// RetailCustomer represents the customer struct as provided
type RetailCustomer struct {
	RetailCustomerCode   string
	TitleCode            string
	FirstLastName        string
	GenderCode           int
	IsMarried            bool
	CompanyCode          int
	IdentityNum          string
	RetailCustomerAtt01  string
	ActivationStatusCode byte
	IsPresentCardBlocked bool
	OpenDate             time.Time
}

// TestMainScenario tests the specific scenario from the provided main function
func TestMainScenario(t *testing.T) {
	sqlCondition := "(RetailCustomerCode = 'Customer1') OR (IsMarried = 1 AND GenderCode = 0)"

	// Parse the condition
	condition, err := Parse(sqlCondition)
	if err != nil {
		t.Errorf("Error parsing condition: %v", err)
		return
	}

	// Create a customer
	customer := RetailCustomer{
		RetailCustomerCode: "Customer1",
		GenderCode:         1,
		IsMarried:          false,
		OpenDate:           time.Now(),
	}

	// Evaluate the condition
	result, err := EvaluateCondition(customer, condition)
	if err != nil {
		t.Errorf("Error evaluating condition: %v", err)
		return
	}

	// Check the result
	want := true // Expected result because RetailCustomerCode matches
	if result != want {
		t.Errorf("Customer matches condition: got %v, want %v", result, want)
	} else {
		fmt.Printf("Customer matches condition: %v\n", result)
	}
}