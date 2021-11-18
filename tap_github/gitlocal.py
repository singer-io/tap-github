import subprocess
import sys
import os
import re
import json

class GitLocalException(Exception):
  pass

def parseDiffLines(lines):
  changes = []
  curChange = None
  state = 'start'
  lastDiffLine = ''
  for line in lines:
    if len(line) == 0:
      # Only happens on last line -- other blank lines at least start with space
      if curChange:
        changes.append(curChange)
        curChange = None
      continue
    elif line[0:4] == 'diff': # diff
      lastDiffLine = line
      # For a pure file mode change, the change type will be none without a patch. Convert this to
      # an edit.
      if curChange and (curChange['changetype'] != 'none' or len(curChange['patch']) > 0 or \
          curChange['is_binary'] or curChange['previous_filename']):
        changes.append(curChange)
      curChange = {
        'filename': '',
        'additions': 0,
        'deletions': 0,
        'patch': [],
        'previous_filename': '',
        'is_binary': False,
        'is_large_patch': False,
        'changetype': 'none',
      }
      state = 'start'
      pass
    elif state == 'inpatch':
      if line[0] == '@':
        # Note: this line may have context at the end, which is okay and part of the git difff
        # format.
        curChange['patch'].append(line)
      else:
        if line[0] == '-':
          curChange['deletions'] += 1
        elif line[0] == '+':
          curChange['additions'] += 1
        curChange['patch'].append(line)
    elif line[0] == 'i': # index
      # Ignore file mode changes for now
      pass
    elif line[0] == 's': # similarity index...
      pass
    elif line[0] == 'o': # old mode...
      pass
    elif line[0] == 'n': # new file/mode...
      if line[:8] == 'new mode':
        curChange['changetype'] = 'edit'
      else:
        curChange['changetype'] = 'add'
      # If the file is empty or binary, this is the way to get the file name. If this is a mode
      # change with a rename, then this will get overwritten by the rename lines that follow
      fileNameLen = int((len(lastDiffLine) - len('diff --git a/ b/')) / 2)
      curChange['filename'] = lastDiffLine[-fileNameLen:]
    elif line[0:3] == 'del': # deleted file
      curChange['changetype'] = 'delete'
      # We won't have anything to identify the file other than the first diff line. For this line,
      # the file name will be the same with a/ and b/, so do this trick to figure out its length
      # (which we need to do because the file name can have " b/" inside of it)
      fileNameLen = int((len(lastDiffLine) - len('diff --git a/ b/')) / 2)
      curChange['filename'] = lastDiffLine[-fileNameLen:]
    elif line[0] == 'B': # Binary files dffer
      curChange['is_binary'] = True
      pass
    elif line[0] == 'r': # rename from/to...
      if line[0:12] == 'rename from ':
        curChange['previous_filename'] = line[12:]
      elif line[0:10] == 'rename to ':
        # Need to save this here becuase a pure rename won't have a +++ line
        curChange['filename'] = line[10:]
    elif line[0] == '-':
      pass # Ignore, will be same as rename from if different from changed file name
    elif line[0] == '+':
      # May already be set for a delete or add
      if not curChange['filename']:
        curChange['filename'] = line.replace('+++ b/', '')
      state = 'inpatch'
    else:
      raise GitLocalException('Unexpected line start: "{}"'.format(line))
  for change in changes:
    change['patch'] = '\n'.join(change['patch'])
    if len(change['patch']) == 0:
      change['patch'] = None
    elif len(change['patch']) > 1024 * 1024:
      change['patch'] = None
      change['is_large_patch'] = True

    if (change['is_binary'] or change['is_large_patch'] or change['patch']) \
        and change['changetype'] == 'none':
      change['changetype'] = 'edit'

    # This isn't strictly necessary, but doing it to make sure that the output is exactly the same
    # as when using the API.
    if not change['previous_filename']:
      del change['previous_filename']
    if not change['patch']:
      del change['patch']
  return changes


class GitLocal:
  def __init__(self, config):
    self.token = config['access_token']
    self.workingDir = config['workingDir']
    self.LS_CACHE = {}
    self.INIT_REPO = {}

  def getOrgWorkingDir(self, repo):
    orgName = repo.split('/')[0]
    orgWdir = '{}/{}'.format(self.workingDir, orgName)
    if not os.path.exists(orgWdir):
      os.mkdir(orgWdir)
    return orgWdir

  def getRepoWorkingDir(self, repo):
    orgDir = self.getOrgWorkingDir(repo)
    repoDir = repo.split('/')[1]
    return '{}/{}'.format(orgDir, repoDir)

  def cloneRepo(self, repo, failOnExists=False):
    """
    Clones a repository using git clone, throwing an error if the operation does not succeed
    """
    if os.path.exists(self.getRepoWorkingDir(repo)):
      if failOnExists:
        raise GitLocalException("Attempted to clone repo {} that already exists".format(repo))
      else:
        return

    cloneUrl = "https://{}@github.com/{}.git".format(self.token, repo)
    orgDir = self.getOrgWorkingDir(repo)
    completed = subprocess.run(['git', 'clone', cloneUrl], cwd=orgDir, capture_output=True)
    if completed.returncode != 0:
      # Don't send the acces token through the error logging system
      strippedOutput = completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("Clone of repo {} failed with code {}, message: {}".format(repo,
        completed.returncode, strippedOutput))

  def initRepo(self, repo):
    if repo in self.INIT_REPO:
      return

    self.cloneRepo(repo)

    # In case a previous run was killed in the middle of anything, do a hard reset of the repo
    # directory
    repoDir = self.getRepoWorkingDir(repo)
    subprocess.run(['git', 'clean', '-fd'], cwd=repoDir, capture_output=True)
    subprocess.run(['git', 'reset', '--hard'], cwd=repoDir, capture_output=True)

    self.INIT_REPO[repo] = True

  def checkoutCommit(self, repo, sha, raiseOnError=True):
    """
    Checks out a commit, returning True if successful and False on failure. Will throw an exception
    if there's a failure unless raiseOnError is set to False.
    """
    # Clone if necessary
    self.initRepo(repo)
    repoDir = self.getRepoWorkingDir(repo)
    completed = subprocess.run(['git', 'checkout', sha], cwd=repoDir, capture_output=True)
    if completed.returncode != 0:
      if not raiseOnError:
        return False
      # Don't send the acces token through the error logging system
      strippedOutput = completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("Checkout of repo {}, sha {} failed with code {}, message: {}".format(
        repo, sha, completed.returncode, strippedOutput))
    return True

  def lsRemote(self, repo):
    if repo in self.LS_CACHE:
      return self.LS_CACHE[repo]

    self.initRepo(repo)
    repoDir = self.getRepoWorkingDir(repo)
    completed = subprocess.run(['git', 'ls-remote'], cwd=repoDir, capture_output=True)
    if completed.returncode != 0:
      # Don't send the acces token through the error logging system
      strippedOutput = completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("ls-remote of repo {} failed with code {}, message: {}".format(
        repo, completed.returncode, strippedOutput))

    outstr = completed.stdout.decode('utf8')

    refmap = {}
    for m in re.finditer( r'([0-9a-f]{40})\s+([^\n]+)(.*?)', outstr):
      hash = m.group(1)
      ref = m.group(2)
      refmap[ref] = hash

    self.LS_CACHE[repo] = refmap
    return refmap

  def fetchRemote(self, repo, ref):
    self.initRepo(repo)
    repoDir = self.getRepoWorkingDir(repo)
    sys.stderr.write('fetching remote ref {}\n'.format(ref))
    completed = subprocess.run(['git', 'fetch', 'origin', ref], cwd=repoDir, capture_output=True)
    if completed.returncode != 0:
      # Don't send the acces token through the error logging system
      strippedOutput = completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("Fetch from origin of repo {}, ref {} failed with code {}, "\
        "message: {}".format(repo, ref, completed.returncode, strippedOutput))

  def fetchRef(self, repo, ref, sha):
    """
    Attempt to fetch a named ref that is or was associated with a particular SHA.
    """
    # First, just try to check out the commit from what we already have
    success = self.checkoutCommit(repo, sha, False)
    if success:
      return True

    # Now see if the ref is in the remote ref map
    refmap = self.lsRemote(repo)
    # If it is, fetch it
    if ref in refmap:
      self.fetchRemote(repo, ref)

    # Now try checking out the sha again
    success = self.checkoutCommit(repo, sha, False)
    return success

  def getCommitsFromHead(self, repo, headSha):
    """
    This function lists multiple commits, but it has a few limitations based on missing data from
    github: (1) it can't fill in the comment count, (2) it doesn't know the github user IDs and
    user names associated wtih the commit.
    """
    self.checkoutCommit(repo, headSha)
    repoDir = self.getRepoWorkingDir(repo)
    # Since git log can't escape stuff, create unique sentinals
    startTok = 'xstart5147587x'
    sepTok = 'xsep4983782x'
    completed = subprocess.run(['git', 'log', '--pretty={}{}'.format(
      startTok,
      sepTok.join(['%H','%T','%P','%an','%ae','%ai','%cn','%ce','%ci','%B'])
    )], cwd=repoDir, capture_output=True)
    if completed.returncode != 0:
      # Don't send the acces token through the error logging system
      strippedOutput = completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("Log of repo {}, sha {} failed with code {}, message: {}".format(
        repo, headSha, completed.returncode, strippedOutput))
    outstr = completed.stdout.decode('utf8')
    commitLines = outstr.split(startTok)
    commits = []
    for rawCommit in commitLines:
      # Strip off trailing newline as well
      split = rawCommit.rstrip().split(sepTok)
      if len(split) < 2:
        continue
      commits.append({
        '_sdc_repository': repo,
        'sha': split[0],
        'commit': {
          'author': {
            'name': split[3],
            'email': split[4],
            'date': split[5],
          },
          'committer': {
            'name': split[6],
            'email': split[7],
            'date': split[8],
          },
          'tree': {
            'sha': split[1],
            'url': 'https://api.github.com/repos/{}//git/trees/{}'.format(repo, split[1]),
          },
          'url': 'https://api.github.com/repos/{}/commits/{}'.format(repo, split[0]),
          # Omit comment_count, since comments are a github thing
          # Omit 'verification' since we don't care about signatures right now
        },
        # Omit node_id, since we strip it out
        'url': 'https://api.github.com/repos/{}/commits/{}'.format(repo, split[0]),
        'html_url': 'https://api.github.com/repos/{}/commits/{}'.format(repo, split[0]),
        'comments_url': 'https://api.github.com/repos/{}/commits/{}/comments'.format(repo, split[0]),
        # author and committer may also exist here in github along with info about github user IDs.
        # We can't include those becuase we don't know them.
        # TODO: import these somehow, since they are the one and only mapping between github user
        # IDs and what is in the git log.
        'parents': [] if split[2] == '' else ({
          'sha': p,
          'url': 'https://api.github.com/repos/{}/commits/{}'.format(repo, p),
          'html_url': 'https://github.com/{}/commit/{}'.format(repo, p)
        } for p in split[2].split(' ')),
        # Leave stats empty -- it isn't included when listing multiple commits
        # Leave files empty -- it isn't included when listing multiple commits
      })
    return commits

  def getCommitDiff(self, repo, sha):
    """
    Gets detailed information about a commit at a particular sha
    """
    #self.checkoutCommit(repo, sha)
    repoDir = self.getRepoWorkingDir(repo)
    completed = subprocess.run(['git', 'diff', sha + '~1', sha], cwd=repoDir, capture_output=True)
    # Special case -- first commit, diff instead with an empty tree
    if completed.returncode != 0 and b"~1': unknown revision or path not in the working tree" \
        in completed.stderr:
      # 4b825dc642cb6eb9a060e54bf8d69288fbee4904 is the sha of the empty tree
      completed = subprocess.run(['git', 'diff', '4b825dc642cb6eb9a060e54bf8d69288fbee4904',
        sha], cwd=repoDir, capture_output=True)
    if completed.returncode != 0:
      # Don't send the acces token through the error logging system
      strippedOutput = '' if not completed.stderr else \
        completed.stderr.replace(self.token.encode('utf8'), b'<TOKEN>')
      raise GitLocalException("Diff of repo {}, sha {} failed with code {}, message: {}".format(
        repo, sha, completed.returncode, strippedOutput))

    outstr = completed.stdout.decode('utf8')
    lines = outstr.split('\n')

    parsed = parseDiffLines(lines)
    for diff in parsed:
      diff['commit_sha'] = sha

    return parsed
