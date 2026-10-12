package elastic

import (
	"fmt"
	"regexp"
	"strconv"
)

var releaseVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func parseReleaseVersion(version string) (major, minor int, err error) {
	parts := releaseVersion.FindStringSubmatch(version)
	if parts == nil {
		return 0, 0, fmt.Errorf("invalid or prerelease Elasticsearch version %q", version)
	}
	major, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Elasticsearch version %q", version)
	}
	minor, err = strconv.Atoi(parts[2])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Elasticsearch version %q", version)
	}
	return major, minor, nil
}

// ValidateMigrationVersions checks the REST API boundary, not index-specific
// mapping/settings compatibility (which is checked when the copy is created).
// 7.14 is the first 7.x release with the product header required by our client;
// it also includes the acknowledged write-block and composable-template APIs.
func ValidateMigrationVersions(source, destination string) error {
	majors := [2]int{}
	for i, version := range []string{source, destination} {
		major, minor, err := parseReleaseVersion(version)
		if err != nil {
			return err
		}
		if major < 7 || major > 9 || (major == 7 && minor < 14) {
			return fmt.Errorf("unsupported Elasticsearch version %q; migration supports matching major versions: 7.14+, 8.x, or 9.x", version)
		}
		majors[i] = major
	}
	if majors[0] != majors[1] {
		return fmt.Errorf("cross-major migration is not supported: source=%s destination=%s; use 7 to 7, 8 to 8, or 9 to 9", source, destination)
	}
	return nil
}
