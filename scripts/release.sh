#!/bin/bash

# Pinot Driver Release Script
# This script helps trigger GitHub releases for the Pinot driver

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] <version> <commit_sha>

Creates a GitHub release for the Pinot driver.

Arguments:
  version     Release version in semver format (e.g., 1.0.0, 1.0.0-alpha.1)
  commit_sha  Full commit SHA to release from

Options:
  -p, --prerelease    Mark as prerelease
  -h, --help         Show this help message

Examples:
  $0 1.0.0 abc123def456...
  $0 --prerelease 1.0.0-alpha.1 abc123def456...

Note: This script requires GitHub CLI (gh) to be installed and authenticated.
EOF
}

# Function to validate version format
validate_version() {
    local version=$1
    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
        print_error "Invalid version format: $version"
        print_info "Expected semver format (e.g., 1.0.0 or 1.0.0-alpha.1)"
        return 1
    fi
    return 0
}

# Function to validate commit SHA
validate_commit() {
    local commit_sha=$1
    if ! git rev-parse --verify "$commit_sha" >/dev/null 2>&1; then
        print_error "Commit SHA not found: $commit_sha"
        print_info "Make sure you're in the correct repository and the commit exists"
        return 1
    fi
    return 0
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir >/dev/null 2>&1; then
        print_error "Not in a git repository"
        return 1
    fi
    
    # Check if GitHub CLI is installed
    if ! command -v gh >/dev/null 2>&1; then
        print_error "GitHub CLI (gh) is not installed"
        print_info "Install it from: https://cli.github.com/"
        return 1
    fi
    
    # Check if GitHub CLI is authenticated
    if ! gh auth status >/dev/null 2>&1; then
        print_error "GitHub CLI is not authenticated"
        print_info "Run: gh auth login"
        return 1
    fi
    
    print_success "All prerequisites met"
    return 0
}

# Function to show release information
show_release_info() {
    local version=$1
    local commit_sha=$2
    local prerelease=$3
    local commit_msg
    
    commit_msg=$(git log --oneline -1 "$commit_sha" 2>/dev/null || echo "Unknown commit")
    
    echo
    print_info "Release Information:"
    echo "  Version: $version"
    echo "  Commit: $commit_sha"
    echo "  Commit Message: $commit_msg"
    echo "  Prerelease: $prerelease"
    echo "  Repository: $(git remote get-url origin 2>/dev/null || echo 'Unknown')"
    echo
}

# Function to trigger the release
trigger_release() {
    local version=$1
    local commit_sha=$2
    local prerelease=$3
    
    print_info "Triggering GitHub release workflow..."
    
    # Prepare the workflow dispatch command
    local workflow_inputs="version=$version,commit_sha=$commit_sha,prerelease=$prerelease"
    
    if gh workflow run release.yml --field="$workflow_inputs"; then
        print_success "Release workflow triggered successfully!"
        print_info "You can monitor the progress at:"
        echo "  https://github.com/$(gh repo view --json owner,name --jq '.owner.login + "/" + .name')/actions"
        echo
        print_info "The release will be available at:"
        echo "  https://github.com/$(gh repo view --json owner,name --jq '.owner.login + "/" + .name')/releases/tag/v$version"
    else
        print_error "Failed to trigger release workflow"
        return 1
    fi
}

# Main function
main() {
    local version=""
    local commit_sha=""
    local prerelease="false"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--prerelease)
                prerelease="true"
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            -*)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [[ -z "$version" ]]; then
                    version=$1
                elif [[ -z "$commit_sha" ]]; then
                    commit_sha=$1
                else
                    print_error "Too many arguments"
                    show_usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Check if required arguments are provided
    if [[ -z "$version" || -z "$commit_sha" ]]; then
        print_error "Missing required arguments"
        show_usage
        exit 1
    fi
    
    # Check prerequisites
    if ! check_prerequisites; then
        exit 1
    fi
    
    # Validate inputs
    if ! validate_version "$version"; then
        exit 1
    fi
    
    if ! validate_commit "$commit_sha"; then
        exit 1
    fi
    
    # Show release information
    show_release_info "$version" "$commit_sha" "$prerelease"
    
    # Confirm with user
    print_warning "This will create a release. Are you sure you want to continue? (y/N)"
    read -r confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Release cancelled"
        exit 0
    fi
    
    # Trigger the release
    trigger_release "$version" "$commit_sha" "$prerelease"
}

# Run main function
main "$@" 