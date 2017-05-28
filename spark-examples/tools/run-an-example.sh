#!/bin/bash

cd "${0%/*}/.."

main() {
  local MAIN_JAR="target/spark-examples-0.0.1-jar-with-dependencies.jar"

  choose() {
    #prompt msg; options array; return var; optional chosen index
    declare -a opts=("${!2}")
    local opt
    if [ "$4" -le "${#opts[@]}" -a "$4" -gt 0 ] 2>/dev/null; then
      opt="${opts[$4-1]}"
      echo $opt
    else
      echo "$1"
      select opt in "${opts[@]}" "exit"; do
        case $opt in
          "exit") echo "Bye"; return 1;;
          "") echo "Invalid option!"; return 2;;
          *) break;;
        esac
      done
    fi
    eval "$3=$opt"
  }

  local classes=($(\
    cd src/main/java; \
    find . -type f -name '*.java' -exec grep -l 'public static void main(' {} \; \
      |sort|sed -r 's#^\./|\.java$##g'|tr / . \
  ))

  local class
  choose "Pick a classe to execute:" classes[@] class "$1" || return $(($?-1))

  local envs=("Locally" "Dataproc")
  local env
  choose "Where do you want to execute?" envs[@] env "$2" || return $(($?-1))
  
  if [ "$env" == "Dataproc" ]; then
    gcloud dataproc jobs submit spark \
      --cluster=spark-demo --class $class --jars "$MAIN_JAR"
  else
    spark-submit --class $class --master local[2] "$MAIN_JAR" 2> err.tmp
  fi
}

main "$@"
