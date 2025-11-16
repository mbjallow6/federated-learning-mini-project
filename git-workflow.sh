#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage
usage() {
    echo "Usage: ./git-workflow.sh <command> [commit-message]"
    echo ""
    echo "Commands:"
    echo "  save    - Add all changes and commit (requires message)"
    echo "  push    - Save, pull latest, and push to remote"
    echo "  sync    - Pull latest changes from current branch"
    echo "  status  - Show detailed status"
    echo "  check   - Run sanity checks without committing"
    echo "  resolve - Continue after resolving conflicts"
    echo "  abort   - Abort rebase and return to previous state"
    echo ""
    echo "Examples:"
    echo "  ./git-workflow.sh save 'Added new ETL feature'"
    echo "  ./git-workflow.sh push 'Fixed bug in mapper'"
    echo "  ./git-workflow.sh sync"
}

# Sanity check function
sanity_check() {
    echo -e "${YELLOW}Running sanity checks...${NC}"

    # Check if in git repository
    if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
        echo -e "${RED}Error: Not in a git repository${NC}"
        exit 1
    fi

    # Check if in middle of rebase
    if [ -d ".git/rebase-merge" ] || [ -d ".git/rebase-apply" ]; then
        echo -e "${RED}⚠️  You are in the middle of a rebase!${NC}"
        echo -e "${YELLOW}Options:${NC}"
        echo -e "  1. Resolve conflicts, then run: ${BLUE}./git-workflow.sh resolve${NC}"
        echo -e "  2. Abort rebase and go back: ${BLUE}./git-workflow.sh abort${NC}"
        exit 1
    fi

    # Get current branch
    BRANCH=$(git branch --show-current)
    echo -e "${GREEN}✓ Current branch: $BRANCH${NC}"

    # Check for uncommitted changes
    if [[ -n $(git status -s) ]]; then
        echo -e "${YELLOW}⚠️  You have uncommitted changes:${NC}"
        git status -s
    else
        echo -e "${GREEN}✓ Working directory is clean${NC}"
    fi

    # Check connection to remote
    if git ls-remote origin > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Connected to remote repository${NC}"
    else
        echo -e "${RED}✗ Cannot connect to remote repository${NC}"
        exit 1
    fi

    # Check if remote is ahead
    git fetch origin "$BRANCH" 2>/dev/null
    LOCAL=$(git rev-parse @)
    REMOTE=$(git rev-parse @{u} 2>/dev/null)
    BASE=$(git merge-base @ @{u} 2>/dev/null)

    if [ "$LOCAL" = "$REMOTE" ]; then
        echo -e "${GREEN}✓ Local and remote are in sync${NC}"
    elif [ "$LOCAL" = "$BASE" ]; then
        echo -e "${YELLOW}⚠️  Remote has new commits (you need to pull)${NC}"
    elif [ "$REMOTE" = "$BASE" ]; then
        echo -e "${YELLOW}⚠️  You have local commits not pushed${NC}"
    else
        echo -e "${YELLOW}⚠️  Local and remote have diverged${NC}"
    fi
}

# Save command (add + commit) - FIXED VERSION
do_save() {
    if [ -z "$1" ]; then
        echo -e "${RED}Error: Commit message required${NC}"
        echo "Usage: ./git-workflow.sh save 'your commit message'"
        exit 1
    fi

    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  SAVING CHANGES${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    echo -e "${YELLOW}Adding all changes...${NC}"
    git add -A

    echo -e "${YELLOW}Committing with message: $1${NC}"

    # Try to commit
    if git commit -m "$1"; then
        echo -e "${GREEN}✓ Changes committed successfully!${NC}"
    else
        # Commit failed - likely due to pre-commit hooks modifying files
        echo -e "${YELLOW}⚠️  Pre-commit hooks modified files${NC}"

        # Check if there are modified files that need to be re-added
        if [[ -n $(git status -s) ]]; then
            echo -e "${YELLOW}Re-adding modified files...${NC}"
            git add -A

            echo -e "${YELLOW}Retrying commit...${NC}"
            if git commit -m "$1"; then
                echo -e "${GREEN}✓ Changes committed successfully after pre-commit fixes!${NC}"
            else
                # Still failed after retry
                echo -e "${RED}✗ Commit failed even after retry${NC}"
                echo -e "${YELLOW}This might indicate a real error in your code${NC}"
                echo ""
                echo -e "${YELLOW}You can:${NC}"
                echo "  1. Check the error messages above"
                echo "  2. Fix any issues manually"
                echo "  3. Run the command again"
                exit 1
            fi
        else
            # No modified files but commit still failed
            echo -e "${RED}✗ Commit failed${NC}"
            echo -e "${YELLOW}Check the error messages above${NC}"
            exit 1
        fi
    fi
}

# Sync command (pull latest)
do_sync() {
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  SYNCING WITH REMOTE${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    BRANCH=$(git branch --show-current)
    echo -e "${YELLOW}Fetching latest from origin/$BRANCH...${NC}"
    git fetch origin "$BRANCH"

    # Check what we're pulling
    COMMITS_BEHIND=$(git rev-list --count HEAD..origin/$BRANCH 2>/dev/null)
    if [ "$COMMITS_BEHIND" -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Remote has $COMMITS_BEHIND new commit(s)${NC}"
        echo -e "${YELLOW}Recent remote commits:${NC}"
        git log --oneline HEAD..origin/$BRANCH | head -5
    else
        echo -e "${GREEN}✓ Already up to date with remote${NC}"
        return 0
    fi

    echo -e "${YELLOW}Pulling with rebase...${NC}"
    if git pull --rebase origin "$BRANCH"; then
        echo -e "${GREEN}✓ Successfully synced with remote!${NC}"
        return 0
    else
        echo -e "${RED}═══════════════════════════════════════${NC}"
        echo -e "${RED}  ⚠️  MERGE CONFLICT DETECTED${NC}"
        echo -e "${RED}═══════════════════════════════════════${NC}"
        echo ""
        echo -e "${YELLOW}Conflicting files:${NC}"
        git diff --name-only --diff-filter=U
        echo ""
        echo -e "${YELLOW}What happened:${NC}"
        echo "  Changes from another computer conflict with your local changes."
        echo ""
        echo -e "${YELLOW}To resolve:${NC}"
        echo "  1. Open conflicting files in your editor"
        echo "  2. Look for conflict markers: <<<<<<< ======= >>>>>>>"
        echo "  3. Edit files to resolve conflicts"
        echo "  4. Run: ${BLUE}git add <file>${NC} for each resolved file"
        echo "  5. Run: ${BLUE}./git-workflow.sh resolve${NC}"
        echo ""
        echo -e "${YELLOW}To abort and go back:${NC}"
        echo "  Run: ${BLUE}./git-workflow.sh abort${NC}"
        echo ""
        exit 1
    fi
}

# Resolve command (continue after conflict resolution)
do_resolve() {
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  CONTINUING REBASE${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    # Check if there are still conflicts
    if git diff --name-only --diff-filter=U | grep -q .; then
        echo -e "${RED}✗ You still have unresolved conflicts:${NC}"
        git diff --name-only --diff-filter=U
        echo ""
        echo -e "${YELLOW}Please resolve all conflicts and add them with:${NC}"
        echo -e "  ${BLUE}git add <file>${NC}"
        exit 1
    fi

    echo -e "${YELLOW}Continuing rebase...${NC}"
    if git rebase --continue; then
        echo -e "${GREEN}✓ Conflicts resolved! Rebase completed.${NC}"
        echo ""
        echo -e "${YELLOW}Now you can push your changes:${NC}"
        echo -e "  ${BLUE}git push origin $(git branch --show-current)${NC}"
    else
        echo -e "${RED}✗ Error continuing rebase${NC}"
        exit 1
    fi
}

# Abort command (abort rebase)
do_abort() {
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  ABORTING REBASE${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    if git rebase --abort; then
        echo -e "${GREEN}✓ Rebase aborted. Returned to previous state.${NC}"
    else
        echo -e "${RED}✗ Error aborting rebase${NC}"
        exit 1
    fi
}

# Push command (save + sync + push)
do_push() {
    if [ -z "$1" ]; then
        echo -e "${RED}Error: Commit message required${NC}"
        echo "Usage: ./git-workflow.sh push 'your commit message'"
        exit 1
    fi

    # Save changes first
    do_save "$1"

    echo ""
    # Sync with remote
    do_sync

    echo ""
    # Push to remote
    BRANCH=$(git branch --show-current)
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  PUSHING TO REMOTE${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    echo -e "${YELLOW}Pushing to origin/$BRANCH...${NC}"
    if git push origin "$BRANCH"; then
        echo -e "${GREEN}✓ Successfully pushed to remote!${NC}"
        echo ""
        echo -e "${GREEN}Summary:${NC}"
        echo -e "  • Branch: ${BLUE}$BRANCH${NC}"
        echo -e "  • Message: ${BLUE}$1${NC}"
        if [ "$BRANCH" != "main" ]; then
            echo ""
            echo -e "${YELLOW}Note: Changes are in $BRANCH branch.${NC}"
            echo -e "${YELLOW}Create a PR to merge to main when ready.${NC}"
        fi
    else
        echo -e "${RED}✗ Push failed!${NC}"
        echo ""
        echo -e "${YELLOW}This might mean:${NC}"
        echo "  • Someone pushed to remote while you were working"
        echo "  • You need to pull and resolve conflicts first"
        echo ""
        echo -e "${YELLOW}Try running:${NC}"
        echo -e "  ${BLUE}./git-workflow.sh sync${NC}"
        exit 1
    fi
}

# Status command
do_status() {
    sanity_check
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  DETAILED STATUS${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    git status
    echo ""
    echo -e "${YELLOW}Last 5 commits:${NC}"
    git log --oneline --graph -5
}

# Main command handler
case "$1" in
    save)
        do_save "$2"
        ;;
    push)
        do_push "$2"
        ;;
    sync)
        sanity_check
        echo ""
        do_sync
        ;;
    status)
        do_status
        ;;
    check)
        sanity_check
        ;;
    resolve)
        do_resolve
        ;;
    abort)
        do_abort
        ;;
    *)
        usage
        exit 1
        ;;
esac
