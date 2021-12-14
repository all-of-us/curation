# this _should_ make powershell stop on first error, but apparently there are lots of edge cases.  yay.
$ErrorActionPreference = "Stop"

function in_ci
{
  return (Test-Path "env:CI") -and ($env:CI -eq "true")
}

function missing_required_env
{
  param ([string]$name)

  write-host "Required environment variable ""$name"" is missing or empty"
  exit 1
}

if (($args.Count) -eq 0)
{
  echo "At least one argument must be provided, and one must be the name of a service within docker-compose.yml"
  exit 1
}

# args defined in docker-compose.yml
$uid=1000
$gid=1000
$user=(split-path -path $(Get-WMIObject -class Win32_ComputerSystem | select username).username -leaf)

write-host "Running as user ""$user""..."

$pwd=(get-location)

$dkr_run_args = @(
  "compose",
  "run",
  "-v", """${pwd}/.git:/home/curation/project/curation/.git""",
  "-v", """${pwd}/data_steward:/home/curation/project/curation/data_steward""",
  "-v", """${pwd}/tests:/home/curation/project/curation/tests""",
  "-v", """${pwd}/tools:/home/curation/project/curation/tools""",
  "-v", """${pwd}/.circleci:/home/curation/project/curation/.circleci"""
)

# If running specific tests
if ((Test-Path "env:CURATION_TESTS_FILEPATH") -and (-not ($env:CURATION_TESTS_FILEPATH -eq "") ))
{
  # If env var is set containing test filepaths, include it and update paths to relative paths
  (Get-Content "env:CURATION_TESTS_FILEPATH").replace('.*curation', '.') | Set-Content "env:CURATION_TESTS_FILEPATH"
  $tests_path = (Resolve-Path "env:CURATION_TESTS_FILEPATH")
  $dkr_run_args += "-v"
  $dkr_run_args += """${tests_path}:/home/curation/project/curation/tests/tests-to-run"""
  $dkr_run_args += "-e"
  $dkr_run_args += """CURATION_TESTS_FILEPATH=/home/curation/project/curation/tests/tests-to-run"""
}

if (-not (in_ci))
{
  write-host "Running outside of CI"

  if ((-not (Test-Path "env:GOOGLE_APPLICATION_CREDENTIALS")) -or ($env:GOOGLE_APPLICATION_CREDENTIALS -eq ""))
  {
    (missing_required_env "GOOGLE_APPLICATION_CREDENTIALS")
  }

  $gcreds=$env:GOOGLE_APPLICATION_CREDENTIALS

  write-host "Ensuring image is up to date..."

  $build_args=@(
    "compose",
    "build",
    "--build-arg", "UID=${uid}",
    "--build-arg", "GID=${gid}",
    "--quiet",
    "develop"
  )

  & docker @build_args
  if ($lastexitcode -ne 0)
  {
    write-host "Error(s) occurred while building image"
    exit 1
  }

  # add gcreds path volume mount to runtime args
  $dkr_run_args += "-v"
  $dkr_run_args += """${gcreds}:/home/curation/project/curation/aou-res-curation-test.json"""
}
else
{
  write-host "Running in CI."
}

# finally, add any / all remaining args provided to this script as args to pass into docker

# If the arg list contains "--", assume this to be the separation point between flags to send to
# docker compose, and the container entrypoint command.
#
# Otherwise, assume entire list is to be sent to container entrypoint
#
# This is necessary as we need to inject the name of the service defined within docker-compose.yaml that we want to
# run in-between the flags intended for `docker compose run` and container entrypoint.
if ($args -match "--")
{
  $at_command=0
  foreach ($a in $args)
  {
    if (($a -eq "--") -and ($at_command -eq 0))
    {
      $at_command=1
      $dkr_run_args+="develop"
    }
    else
    {
      $dkr_run_args += $a
    }
  }
}
else
{
  $dkr_run_args+="develop"
  foreach ($a in $args)
  {
    $dkr_run_args += $a
  }
}

# do the thing.
& docker @dkr_run_args
