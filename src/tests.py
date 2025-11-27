# import argparse
# import csv
import itertools
import jinja2
# import json
# import logging
# import os
import re
# import shlex
# import subprocess
import yaml
# import sys
# import time
# 
from pathlib import Path
from jinja2 import Template, Environment, Undefined
# from typing import List, Dict, Any, Optional, Tuple, Callable
# 


def convert_snippets(raw_snip):
    """
    Convert list of snippets: [{name, content}]
    To dicts like: { 'snippet': { name: content } }
    """
    result = {}

    for item in raw_snip:
        name = item["name"]
        content = item["content"]

        if name in result:
            raise ValueError(f"Duplicate snippet name: {name}")

        result[name] = content

    return {"snippet": result}


class KeepUndefined(Undefined):
    def __str__(self):
        # Rebuild full dotted name if possible
        if self._undefined_obj is not None and not isinstance(self._undefined_obj, Undefined):
            full = f"{self._undefined_obj}.{self._undefined_name}"
        else:
            full = self._undefined_name or "undefined"

        return f"{{{{ {full} }}}}"

    __repr__ = __str__
    __html__ = __str__



def render_strings(strings, context):

    if context == None:
        return strings

    env = Environment(undefined=KeepUndefined)
    result = [env.from_string(s).render(**context) for s in strings]

    return result


class TestSet:

    def __init__(self, args, log):

        self.log = log

        self.dir = args.dir
        self.tag = args.tag
        self.max_time = args.maxtime
        self.output_dir = args.output
        self.dry_run = args.dry_run
        self.extra_args = args.extra_args

        self.load_tests()
        
    def load_tests(self):
    
        tests = []

        for tp in sorted(Path(self.dir).glob("*.yaml")):

           test = Test(self.log, self, tp)

           if self.tag and self.tag not in test.tags:
               self.log.debug(f'Skipping test "{test.name}" because it does not match tag "{self.tag}"')
               continue

           tests.append(test)

        if(len(tests) == 0):
            self.log.fatal("No tests loaded")

        self.tests = tests

        self.log.debug(f"Loaded {len(self.tests)} tests")

    def run(self):
        for test in self.tests:
            self.log.info(f'Running test "{test.name}"')
            test.run()


class Test:

    EXTRA_VAR_RE = re.compile(r"\{\{\s*([A-Za-z0-9_\.]+)\s*\}\}")
    SNIPPET_TOKEN_RE = re.compile(r"^\{\{\s*snippet\.([A-Za-z0-9_-]+)\s*\}\}$")

    def __init__(self, log, tset, tp):

        self.file = tp.name
        self.extra_args = tset.extra_args
        self.spec = yaml.safe_load(tp.read_text())

        self.name = self.spec["name"]

        self.desc = self.spec.get("description",  None)
        self.env = self.spec.get("env", {})
        self.tags = self.spec.get("tags", [])
        self.snippets = self.spec.get("snippets", None)

        self.parse = self.load_fn("parse")
        self.validate = self.load_fn("validate")

        self.params = self.param_permutations()
        self.tasks = self.generate_tasks()

    def load_fn(self, fn_name):

        if fn_name not in self.spec:
            return None

        fn_code = self.spec[fn_name]
        fn_ns = {}
        exec(fn_code, fn_ns, fn_ns)
        fn = fn_ns.get(fn_name)

        if not callable(fn):
            raise ValueError(f"`{func_name}` block must define a function named `{func_name}`")

        return fn



    def param_permutations(self):

        params = self.spec.get("params", {})

        keys = params.keys()
        values = [v for v in itertools.product(*params.values())]
        permutations = [dict(zip(keys, x)) for x in values]

        return permutations


    def generate_tasks(self):

        self.tasks = []
        
        for params in self.params:

            env_vars = dict(self.env or {})
            env_vars.update(params)

            command = render_strings(self.spec["command"], convert_snippets(self.snippets))


#command:
#  - srun
#  - --mpi=pmi2
#  - --gpus-per-node 8
#  - --ntasks 2
#  - --nodes 1
#  - --partition {{ extra_args.partition }}
#  - --account {{ extra_args.account }}
#  - singularity
#  - run
#  - -B /boot/config-5.14.21-150500.55.49_13.0.56-cray_shasta_c
#  - -B /pfs/lustrep3/users/viahlgre
#  - "{{ extra_args.sif }}"
#  - bash -c
#  - "{{ snippet.script }}"
#
#snippets:
#  - name: script
#    content: |
#      if [[ $PMI_RANK -eq 0 ]]; then
#        export ROCR_VISIBLE_DEVICES=${rank0_gcd};
#      elif [[ $PMI_RANK -eq 1 ]]; then
#        export ROCR_VISIBLE_DEVICES={{ rank1_gcd }};
#      else
#        echo "unexpected rank $PMI_RANK" >&2; exit 1;
#      fi
#      /usr/libexec/osu-micro-benchmarks/mpi/pt2pt/osu_bw -m ${transfer_size}:${transfer_size} D D
#
#        
#
    def run(self):
        for task in self.tasks:
            print(task)

        
        for params in self.params:

            env_vars.update(params)

            ctx = {"extra_args": self.extra_args}
            ctx.update(params)

    #command = spec.command or []
    #if not command:
    #    raise ValueError("`command` must be a non-empty list")
    #sn_map = {s["name"]: render_string(s["content"], ctx) for s in spec.snippets or []}
    #argv = []
    #for item in command:
    #    raw = str(item).strip()
    #    m = SNIPPET_TOKEN_RE.match(raw)
    #    if (m):
    #        sn_name = m.group(1)
    #        if sn_name not in sn_map:
    #            raise ValueError(f"snippet '{sn_name}' not found")
    #        argv.append(sn_map[sn_name])
    #    else:
    #        rendered = render_string(raw, ctx)
    #        argv.extend(shlex.split(rendered))
    #return argv
            #try:
            #    argv = render_command_to_argv(spec, ctx)
            #except Exception as e:
            #    line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys) if pkeys else ""
            #    logging.error(f"{line} [FAIL] spec error: {e}")
            #    write_perflog(args.prefix, args.sysenv, name, params, 0.0, False, f"spec error: {e}", {})
            #    any_fail = True
            #    continue
            #if args.dry_run:
            #    compact = " ".join(shlex.quote(a) for a in argv)
            #    if pkeys:
            #        line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys)
            #        logging.info(f"{line} DRYRUN {compact}")
            #    else:
            #        logging.info(f"DRYRUN {compact}")
            #    continue
            #try:
            #    rc, out, err, dt = run_argv(argv, env_vars, spec.workdir or ".", args.timeout)
            #except subprocess.TimeoutExpired:
            #    line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys) if pkeys else ""
            #    logging.error(f"{line} [FAIL] timeout")
            #    write_perflog(args.prefix, args.sysenv, name, params, float(args.timeout or 0), False, "timeout", {"rc": "timeout"})
            #    any_fail = True
            #    continue
            #except FileNotFoundError as e:
            #    line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys) if pkeys else ""
            #    logging.error(f"{line} [FAIL] exec error: {e}")
            #    write_perflog(args.prefix, args.sysenv, name, params, 0.0, False, "exec error", {"error": str(e)})
            #    any_fail = True
            #    continue
            #try:
            #    results = parse_fn(out, dict(params)) if parse_fn else {"stdout": out}
            #except Exception as e:
            #    msg = f"parse error: {e}"
            #    line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys) if pkeys else ""
            #    logging.error(f"{line} [FAIL] {msg}")
            #    write_perflog(args.prefix, args.sysenv, name, params, dt, False, msg, {"rc": rc})
            #    any_fail = True
            #    continue
            #passed = (rc == 0)
            #vmsg = ""
            #try:
            #    if validate_fn:
            #        vres = validate_fn(results, dict(params))
            #        if isinstance(vres, tuple) and len(vres) >= 1:
            #            passed = bool(vres[0])
            #            vmsg = "" if len(vres) == 1 else str(vres[1])
            #        else:
            #            passed = bool(vres)
            #    else:
            #        passed = (rc == 0)
            #except Exception as e:
            #    passed = False
            #    vmsg = f"validate error: {e}"
            #if pkeys:
            #    line = " ".join(f"{str(params.get(k, '')):>14s}" for k in pkeys)
            #    logging.info(f"{line} [{'PASS' if passed else 'FAIL'}] {vmsg}")
            #else:
            #    logging.info(f"[{'PASS' if passed else 'FAIL'}] {vmsg}")
            #write_perflog(args.prefix, args.sysenv, name, params, dt, passed, vmsg, results)
            #if not passed:
            #    any_fail = True

        #return

    def __str__(self):
        return f"Test(name={self.name}, value={self.spec})"


# # --- Core Logic ---
# def render_string(s: str, ctx: Dict[str, Any]) -> str:
#     """Render a string using the given context."""
#     return Template(s).substitute(ctx)
# 
# 
# def cartesian_params(param_dict: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
#     """Generate all combinations of parameters."""
#     if not param_dict:
#         return [{}]
#     return [dict(zip(param_dict.keys(), values)) for values in itertools.product(*param_dict.values())]
# 
# def run_argv(argv: List[str], env: Dict[str, str], cwd: str, timeout: Optional[int] = None) -> Tuple[int, str, str, float]:
#     """Run a command and return its output, error, and duration."""
#     env_all = os.environ.copy()
#     env_all.update({k: str(v) for k, v in env.items()})
#     t0 = time.time()
#     proc = subprocess.run(
#         argv,
#         cwd=cwd,
#         env=env_all,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         timeout=timeout,
#     )
#     stdout = proc.stdout.decode("utf-8", "replace") if isinstance(proc.stdout, bytes) else proc.stdout
#     stderr = proc.stderr.decode("utf-8", "replace") if isinstance(proc.stderr, bytes) else proc.stderr
#     dt = time.time() - t0
#     return proc.returncode, stdout, stderr, dt
# 

