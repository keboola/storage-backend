defaultBranchLocal() { local exists_in_local=$(git branch --list main); if [[ -z ${exists_in_local} ]]; then echo master; else echo main; fi }
defaultBranchRemote() { local exists_in_local=$(git branch --list -r $1/main); if [[ -z ${exists_in_local} ]]; then echo master; else echo main; fi }
