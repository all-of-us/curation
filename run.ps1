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
  echo "At least one argument must be provided, and it must be the name of a service within docker-compose.yml"
  exit 1
}

# name of service in docker-compose.yml
$service=$args[0]

# args defined in docker-compose.yml
$uid=1000
$gid=1000
$user=(split-path -path $(Get-WMIObject -class Win32_ComputerSystem | select username).username -leaf)

write-host "Running ""$service"" as user ""$user""..."

$pwd=(get-location)

$run_args = @(
  "compose",
  "run",
  "-v", """${pwd}/.git:/home/curation/project/curation/.git""",
  "-v", """${pwd}/data_steward:/home/curation/project/curation/data_steward""",
  "-v", """${pwd}/tests:/home/curation/project/curation/tests""",
  "-v", """${pwd}/tools:/home/curation/project/curation/tools""",
  ${service}
)

if (-not (in_ci))
{
  write-host "Running outside of CI"

  if ((-not (Test-Path "env:GOOGLE_APPLICATION_CREDENTIALS")) -or ($env:GOOGLE_APPLICATION_CREDENTIALS -eq ""))
  {
    (missing_required_env "GOOGLE_APPLICATION_CREDENTIALS")
  }

  #$gcreds=(unixize_path $env:GOOGLE_APPLICATION_CREDENTIALS)
  $gcreds=$env:GOOGLE_APPLICATION_CREDENTIALS

  # write-host "Ensuring base image is up to date..."

  docker compose build --build-arg uid=1000 --build-arg gid=1000 base
  if ($lastexitcode -ne 0)
  {
    write-host "Error(s) occurred while building base image"
    exit 1
  }

  docker compose build ${service}
  if ($lastexitcode -ne 0)
  {
    write-host "Error(s) occurred while building ""$service"" image"
    exit 1
  }

  # add gcreds path volume mount to runtime args
  $run_args += "-v"
  $run_args += """${gcreds}:/home/curation/project/curation/aou-res-curation-test.json"""

#  write-host "Adding volume mount: $volumes"
}
else
{
  write-host "Running in CI."
}

# finally add any / all remaining args provided to this script as args to pass into container
for (($i = 1); $i -lt ($args.count); $i++)
{
  $run_args += $args[$i]
}

# do the thing.
& docker @run_args
