package main

import (
	"testing"

	"github.com/streadway/amqp"
)

func Test_handleMessage(t *testing.T) {
	type args struct {
		msg  *amqp.Delivery
		conn *posConnection
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handleMessage(tt.args.msg, tt.args.conn); (err != nil) != tt.wantErr {
				t.Errorf("handleMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
