#!/usr/bin/env nu

cd $env.FILE_PWD
$env.QL_DESKTOP_LOG = "debug"
let log = ($env.FILE_PWD | path join "ql-lab.log")

"" | save --force $log

do { ^pkill -x ql-router } | complete | ignore
do { ^pkill -f 'foundation-server$' } | complete | ignore

def run-logged [label: string, binary: string, log: string] {
    ^stdbuf -oL -eL $binary o+e>| lines | each {|line|
        let tagged = $"[($label)] ($line)"
        $"($tagged)\n" | save --append $log
        print $tagged
    }
}

def wait-for-log [text: string, log: string] {
    for _ in 1..250 {
        if (open --raw $log | str contains $text) {
            return
        }
        sleep 20ms
    }
    error make {msg: $"timed out waiting for: ($text)"}
}

^cargo build --release --workspace --bins

let binaries = ($env.FILE_PWD | path join "target" "release")
let router = job spawn {
    run-logged "router" ($binaries | path join "ql-router") $log
}
wait-for-log "QL router listening on" $log

let foundation = job spawn {
    run-logged "foundation" ($binaries | path join "foundation-server") $log
}
wait-for-log "Foundation server registered with QL router" $log

run-logged "desktop" ($binaries | path join "desktop-relay") $log
try { job kill $foundation }
try { job kill $router }

print $"Logs saved to ($log)"
