#!/usr/bin/env bash

install_utils() {
  if ! command -v mvn &> /dev/null
  then
      sudo apt-get install maven
  fi

  if ! command -v curl &> /dev/null
  then
      sudo apt-get install curl
  fi
}

start_dependent_daemon() {
  # "Failed to connect to bus: No such file or directory" on clean WSL
  # https://github.com/Microsoft/WSL/issues/2941
  uname=$(uname -a)
  if [[ "$uname" == *"microsoft"* ]]
  then
    status=$(service dbus status)
    if [[ "$status" != *"running"* ]]
    then
      sudo service dbus start
    fi
  fi
}

install_chrome() {
    echo "Installing latest stable google-chrome"

    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo dpkg -i google-chrome*.deb
    sudo apt-get install -f

    google-chrome -version
}

cd /tmp/ || exit

start_dependent_daemon

# find out chrome version
#CHROME_VERSION="$(google-chrome -version | head -n1 | awk -F '[. ]' '{print $3}')"
# for mac
CHROME_VERSION="$(/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome -version | head -n1 | awk -F '[. ]' '{print $3}')"

if [[ "$CHROME_VERSION" == "" ]]; then
    install_chrome
fi

install_utils

cd - || exit
