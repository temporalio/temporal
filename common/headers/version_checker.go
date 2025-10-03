package headers

import (
	"context"
	"strings"

	"github.com/blang/semver/v4"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/metadata"
)

const (
	ClientNameServer        = "temporal-server"
	ClientNameServerHTTP    = "temporal-server-http"
	ClientNameGoSDK         = "temporal-go"
	ClientNameJavaSDK       = "temporal-java"
	ClientNamePHPSDK        = "temporal-php"
	ClientNameTypeScriptSDK = "temporal-typescript"
	ClientNamePythonSDK     = "temporal-python"
	ClientNameCLI           = "temporal-cli"
	ClientNameUI            = "temporal-ui"
	ClientNameNexusGoSDK    = "Nexus-go-sdk"

	// ServerVersion value can be changed by the create-tag Github workflow.
	// If you change the var name or move it, be sure to update the workflow.
	ServerVersion = "1.29.0"

	// SupportedServerVersions is used by CLI and inter role communication.
	SupportedServerVersions = ">=1.0.0 <2.0.0"

	// FeatureFollowsNextRunID means that the client supports following next execution run id for
	// completed/failed/timedout completion events when getting the final result of a workflow.
	FeatureFollowsNextRunID = "follows-next-run-id"
)

var (
	// AllFeatures contains all known features. This list is used as the value of the supported
	// features header for internal server requests. There is an assumption that if a feature is
	// defined, then the server itself supports it.
	AllFeatures = strings.Join([]string{
		FeatureFollowsNextRunID,
	}, SupportedFeaturesHeaderDelim)

	SupportedClients = map[string]string{
		ClientNameGoSDK:         "<2.0.0",
		ClientNameJavaSDK:       "<2.0.0",
		ClientNamePHPSDK:        "<2.0.0",
		ClientNameTypeScriptSDK: "<2.0.0",
		ClientNameCLI:           "<2.0.0",
		ClientNameServer:        "<2.0.0",
		ClientNameUI:            "<3.0.0",
		ClientNameNexusGoSDK:    "<2.0.0",
	}

	internalVersionHeaderPairs = []string{
		ClientNameHeaderName, ClientNameServer,
		ClientVersionHeaderName, ServerVersion,
		SupportedServerVersionsHeaderName, SupportedServerVersions,
		SupportedFeaturesHeaderName, AllFeatures,
	}
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context) error
		ClientSupportsFeature(ctx context.Context, feature string) bool
	}

	versionChecker struct {
		supportedClients      map[string]string
		supportedClientsRange map[string]semver.Range
		serverVersion         semver.Version
	}
)

// NewDefaultVersionChecker constructs a new VersionChecker using default versions from const.
func NewDefaultVersionChecker() *versionChecker {
	return NewVersionChecker(SupportedClients, ServerVersion)
}

// NewVersionChecker constructs a new VersionChecker
func NewVersionChecker(supportedClients map[string]string, serverVersion string) *versionChecker {
	return &versionChecker{
		serverVersion:         semver.MustParse(serverVersion),
		supportedClients:      supportedClients,
		supportedClientsRange: mustParseRanges(supportedClients),
	}
}

// GetClientNameAndVersion extracts SDK name and version from context headers
func GetClientNameAndVersion(ctx context.Context) (string, string) {
	headers := GetValues(ctx, ClientNameHeaderName, ClientVersionHeaderName)
	clientName := headers[0]
	clientVersion := headers[1]
	return clientName, clientVersion
}

// SetVersions sets headers for internal communications.
func SetVersions(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, internalVersionHeaderPairs...)
}

// SetVersionsForTests sets headers as they would be received from the client.
// Must be used in tests only.
func SetVersionsForTests(ctx context.Context, clientVersion, clientName, supportedServerVersions, supportedFeatures string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientNameHeaderName:              clientName,
		ClientVersionHeaderName:           clientVersion,
		SupportedServerVersionsHeaderName: supportedServerVersions,
		SupportedFeaturesHeaderName:       supportedFeatures,
	}))
}

// ClientSupported returns an error if client is unsupported, nil otherwise.
func (vc *versionChecker) ClientSupported(ctx context.Context) error {

	headers := GetValues(ctx, ClientNameHeaderName, ClientVersionHeaderName, SupportedServerVersionsHeaderName)
	clientName := headers[0]
	clientVersion := headers[1]
	supportedServerVersions := headers[2]

	// Validate client version only if it is provided and server knows about this client.
	if clientName != "" && clientVersion != "" {
		if supportedClientRange, ok := vc.supportedClientsRange[clientName]; ok {
			clientVersionParsed, parseErr := semver.Parse(clientVersion)
			if parseErr != nil {
				return serviceerror.NewInvalidArgumentf("Unable to parse client version: %v", parseErr)
			}
			if !supportedClientRange(clientVersionParsed) {
				return serviceerror.NewClientVersionNotSupported(clientVersion, clientName, vc.supportedClients[clientName])
			}
		}
	}

	// Validate supported server version if it is provided.
	if supportedServerVersions != "" {
		supportedServerVersionsParsed, parseErr := semver.ParseRange(supportedServerVersions)
		if parseErr != nil {
			return serviceerror.NewInvalidArgumentf("Unable to parse supported server versions: %v", parseErr)
		}
		if !supportedServerVersionsParsed(vc.serverVersion) {
			return serviceerror.NewServerVersionNotSupported(vc.serverVersion.String(), supportedServerVersions)
		}
	}

	return nil
}

// ClientSupportsFeature returns true if the client reports support for the
// given feature (which should be one of the Feature... constants above).
func (vc *versionChecker) ClientSupportsFeature(ctx context.Context, feature string) bool {
	headers := GetValues(ctx, SupportedFeaturesHeaderName)
	if len(headers) == 0 {
		return false
	}
	for clientFeature := range strings.SplitSeq(headers[0], SupportedFeaturesHeaderDelim) {
		if clientFeature == feature {
			return true
		}
	}
	return false
}

func mustParseRanges(ranges map[string]string) map[string]semver.Range {
	out := make(map[string]semver.Range, len(ranges))
	for c, r := range ranges {
		out[c] = semver.MustParseRange(r)
	}
	return out
}
