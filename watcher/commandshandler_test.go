package watcher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/armosec/armoapi-go/apis"
	utilsmetadata "github.com/armosec/utils-k8s-go/armometadata"
	"github.com/kubescape/backend/pkg/command"
	"github.com/kubescape/backend/pkg/command/types/v1alpha1"
	beUtils "github.com/kubescape/backend/pkg/utils"
	"github.com/kubescape/k8s-interface/k8sinterface"
	"github.com/kubescape/operator/config"
	"github.com/kubescape/operator/utils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestInvokeCommandCarriesTheJobTrackingOfTheBody(t *testing.T) {
	cfg, err := config.NewOperatorConfig(config.CapabilitiesConfig{}, utilsmetadata.ClusterConfig{}, &beUtils.Credentials{}, config.Config{Namespace: "kubescape"})
	require.NoError(t, err)
	sessions := make(chan utils.SessionObj, 1)
	pool, err := ants.NewPoolWithFunc(1, func(i interface{}) {
		job := i.(utils.Job)
		sessions <- job.Obj()
	})
	require.NoError(t, err)
	defer pool.Release()
	handler := &OperatorCommandsHandler{ctx: context.Background(), pool: pool, config: cfg}

	tests := []struct {
		name       string
		tracking   apis.JobTracking
		guid       string
		wantJob    string
		wantParent string
	}{
		{name: "parent id from the body, job id from the GUID", tracking: apis.JobTracking{JobID: "job-1", ParentID: "parent-1"}, guid: "guid-1", wantJob: "guid-1", wantParent: "parent-1"},
		{name: "no tracking in the body", guid: "guid-2", wantJob: "guid-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(apis.Command{CommandName: apis.TypeRunKubescape, JobTracking: tt.tracking})
			require.NoError(t, err)
			handler.invokeCommand(context.Background(), v1alpha1.OperatorCommand{Spec: v1alpha1.OperatorCommandSpec{
				GUID:        tt.guid,
				CommandType: string(command.OperatorCommandTypeOperatorAPI),
				Body:        body,
			}})
			select {
			case session := <-sessions:
				assert.Equal(t, tt.wantJob, session.JobID)
				assert.Equal(t, tt.wantParent, session.ParentJobID)
				assert.Equal(t, apis.TypeRunKubescape, session.Command.CommandName)
			case <-time.After(5 * time.Second):
				t.Fatal("the command was not handed to the worker pool")
			}
		})
	}
}

func TestInvokeCommandPoolRejectionUpdatesCommandStatus(t *testing.T) {
	tests := []struct {
		name        string
		setupPool   func(t *testing.T) *ants.PoolWithFunc
		expectedErr string
	}{
		{
			name: "closed worker pool",
			setupPool: func(t *testing.T) *ants.PoolWithFunc {
				p, err := ants.NewPoolWithFunc(1, func(i interface{}) {})
				require.NoError(t, err)
				p.Release()
				return p
			},
			expectedErr: ants.ErrPoolClosed.Error(),
		},
		{
			name: "overloaded non-blocking worker pool",
			setupPool: func(t *testing.T) *ants.PoolWithFunc {
				blockCh := make(chan struct{})
				t.Cleanup(func() { close(blockCh) })
				p, err := ants.NewPoolWithFunc(1, func(i interface{}) {
					<-blockCh
				}, ants.WithNonblocking(true))
				require.NoError(t, err)
				t.Cleanup(p.Release)
				// Occupy the single worker
				err = p.Invoke(utils.Job{})
				require.NoError(t, err)
				return p
			},
			expectedErr: ants.ErrPoolOverload.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.NewOperatorConfig(config.CapabilitiesConfig{}, utilsmetadata.ClusterConfig{}, &beUtils.Credentials{}, config.Config{Namespace: "kubescape"})
			require.NoError(t, err)

			pool := tt.setupPool(t)

			dynClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
			var patchedRaw []byte
			dynClient.PrependReactor("patch", "operatorcommands", func(action clienttesting.Action) (bool, runtime.Object, error) {
				patchAction, ok := action.(clienttesting.PatchAction)
				require.True(t, ok)
				assert.Equal(t, "status", patchAction.GetSubresource())
				assert.Equal(t, "kubescape", patchAction.GetNamespace())
				assert.Equal(t, "test-cmd", patchAction.GetName())
				patchedRaw = patchAction.GetPatch()
				return true, nil, nil
			})

			k8sAPI := &k8sinterface.KubernetesApi{
				DynamicClient: dynClient,
			}

			handler := &OperatorCommandsHandler{
				ctx:    context.Background(),
				pool:   pool,
				config: cfg,
				k8sAPI: k8sAPI,
			}

			body, err := json.Marshal(apis.Command{CommandName: apis.TypeRunKubescape})
			require.NoError(t, err)

			opcmd := v1alpha1.OperatorCommand{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cmd",
					Namespace: "kubescape",
				},
				Spec: v1alpha1.OperatorCommandSpec{
					GUID:        "guid-pool-rejection",
					CommandType: string(command.OperatorCommandTypeOperatorAPI),
					Body:        body,
				},
			}

			handler.invokeCommand(context.Background(), opcmd)

			require.NotEmpty(t, patchedRaw, "expected OperatorCommand status to be patched on pool rejection")

			var statusPatch struct {
				Status v1alpha1.OperatorCommandStatus `json:"status"`
			}
			err = json.Unmarshal(patchedRaw, &statusPatch)
			require.NoError(t, err)

			assert.True(t, statusPatch.Status.Started)
			assert.True(t, statusPatch.Status.Completed)
			assert.Equal(t, "operator", statusPatch.Status.Executer)
			require.NotNil(t, statusPatch.Status.Error)
			assert.Contains(t, statusPatch.Status.Error.Message, "failed to queue command for execution")
			assert.Contains(t, statusPatch.Status.Error.Message, tt.expectedErr)
		})
	}
}
