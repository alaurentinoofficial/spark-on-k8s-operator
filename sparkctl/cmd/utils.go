/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
)

func getSinceTime(timestamp metav1.Time) string {
	if timestamp.IsZero() {
		return "N.A."
	}

	return duration.ShortHumanDuration(time.Since(timestamp.Time))
}

func formatNotAvailable(info string) string {
	if info == "" {
		return "N.A."
	}
	return info
}

func waitSparkApplicationEnds(
	sparkApplicationName string,
	crdClient crdclientset.Interface,
	timeout time.Duration) error {

	running := make(chan interface{})
	done := make(chan interface{})
	go func() {
		for {
			app, _ := crdClient.SparkoperatorV1beta2().SparkApplications(Namespace).Get(
				context.TODO(),
				sparkApplicationName,
				metav1.GetOptions{})

			if app != nil {
				actual := app.Status.AppState.State

				if actual == v1beta2.RunningState {
					running <- nil
				}

				if actual == v1beta2.CompletedState {
					done <- nil
					return
				} else {
					time.Sleep(200 * time.Millisecond)
				}
			}
		}
	}()

	for {
		select {
		case <-done:
			return nil
		case <-running:
			continue
		case <-time.After(timeout):
			return fmt.Errorf("timeout: SparkApplication not started")
		}
	}
}
