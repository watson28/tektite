package integ

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestAgentCommandMissingRequiredArgs(t *testing.T) {
	testAgentMissingRequiredArgs(t, "", "missing flags: --obj-store-username=STRING, --obj-store-password=STRING, --obj-store-url=STRING, --cluster-name=STRING, --location=STRING")
	testAgentMissingRequiredArgs(t, "--obj-store-username=some-user", "missing flags: --obj-store-password=STRING, --obj-store-url=STRING, --cluster-name=STRING, --location=STRING")
	testAgentMissingRequiredArgs(t, "--obj-store-username=some-user --obj-store-password=some-pwd", "missing flags: --obj-store-url=STRING, --cluster-name=STRING, --location=STRING")
	testAgentMissingRequiredArgs(t, "--obj-store-username=some-user --obj-store-password=some-pwd --obj-store-url=some-url", "missing flags: --cluster-name=STRING, --location=STRING")
	testAgentMissingRequiredArgs(t, "--obj-store-username=some-user --obj-store-password=some-pwd --obj-store-url=some-url --cluster-name=some-cluster", "missing flags: --location=STRING")
	testAgentMissingRequiredArgs(t, "--obj-store-username=some-user --obj-store-password=some-pwd --obj-store-url=some-url --location=some-az", "missing flags: --cluster-name=STRING")
}

func testAgentMissingRequiredArgs(t *testing.T, args string, expectedMsg string) {
	mgr := NewManager()
	out, err := mgr.RunAgentAndGetOutput(args)
	require.NoError(t, err)
	require.Equal(t, 1, len(out))
	require.Equal(t, expectedMsg, out[0])
}

func TestAgentCommandHelp(t *testing.T) {
	expected :=
		`Usage: tekagent --obj-store-username=STRING --obj-store-password=STRING --obj-store-url=STRING --cluster-name=STRING --location=STRING

Flags:
  -h, --help                                         Show context-sensitive help.
      --obj-store-username=STRING                    username for the object store
      --obj-store-password=STRING                    password for the object store
      --obj-store-url=STRING                         url of the object store
      --cluster-name=STRING                          name of the agent cluster
      --location=STRING                              location (e.g. availability zone) that the agent runs in
      --kafka-listen-address=STRING                  address to listen on for kafka connections
      --internal-listen-address=STRING               address to listen on for internal connections
      --membership-update-interval-ms=5000           interval between updating cluster membership in ms
      --membership-eviction-interval-ms=20000        interval after which member will be evicted from the cluster
      --consumer-group-initial-join-delay-ms=3000    initial delay to wait for more consumers to join a new consumer group before performing the first
                                                     rebalance, in ms
      --topic-name=STRING                            name of the topic
      --log-format="console"                         format to write log lines in - one of: console, json
      --log-level="info"                             lowest log level that will be emitted - one of: debug, info, warn, error`

	mgr := NewManager()
	out, err := mgr.RunAgentAndGetOutput("--help")
	require.NoError(t, err)

	require.Equal(t, expected, outputToString(out))
}

func outputToString(out []string) string {
	var sb strings.Builder
	for i, line := range out {
		sb.WriteString(line)
		if i != len(out)-1 {
			sb.WriteRune('\n')
		}
	}
	return sb.String()
}

func TestAgentCommandOutputSuccessfulStartStop(t *testing.T) {
	mgr := NewManager()
	agent, err := mgr.StartAgent("--obj-store-username=minioadmin --obj-store-password=miniopassword --obj-store-url=127.0.0.1:9000 --cluster-name=test-cluster --location=az1", true)
	require.NoError(t, err)

	err = agent.Stop()
	require.NoError(t, err)

	expected :=
		`signal: 'interrupt' received. tektite agent will stop
tektite agent has stopped`

	allOut := agent.Output()
	require.Equal(t, 3, len(allOut))
	require.True(t, strings.HasPrefix(allOut[0], "started tektite agent with kafka listener"))
	require.True(t, strings.Contains(allOut[0], "and internal listener"))
	require.Equal(t, expected, outputToString(allOut[1:]))
}