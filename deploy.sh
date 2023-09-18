function update_branch {
  git branch -D $1
  git branch $1
  git push -f origin refs/heads/$1:refs/heads/$1
}

case $1 in
  dev)
    update_branch $1
    ;;
  qa1)
    update_branch $1
    ;;
  stage)
    update_branch $1
    ;;
  uat)
    update_branch $1
    ;;
  *)
    echo "Invalid environment"
    ;;
esac
