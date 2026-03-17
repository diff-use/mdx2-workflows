#!/usr/bin/env python
"""
Helper script to easily run mdx2 CLI commands via Prefect from the command line.

Usage examples (run from mdx2 repo root):
    # Run a single command
    python prefect/run_mdx2_prefect.py map geom.nxs hkl.nxs --outfile map.nxs
    
    # Run a pipeline of commands
    python prefect/run_mdx2_prefect.py pipeline integrate geom.nxs data.nxs --outfile integrated.nxs
    python prefect/run_mdx2_prefect.py pipeline scale integrated.nxs --outfile scaled.nxs
    python prefect/run_mdx2_prefect.py pipeline map geom.nxs scaled.nxs --outfile map.nxs
"""

import sys
from pathlib import Path

# Ensure prefect/ is on path so prefect_flows can be imported (avoids shadowing prefect library)
_prefect_dir = Path(__file__).resolve().parent
if str(_prefect_dir) not in sys.path:
    sys.path.insert(0, str(_prefect_dir))

from prefect_flows import (
    integrate_flow,
    map_flow,
    mdx2_pipeline_flow,
    run_mdx2_cli_command,
    scale_flow,
)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    args = sys.argv[2:]
    
    if command == "pipeline":
        # For pipeline, collect commands until done
        print("Pipeline mode: collect commands (type 'done' when finished)")
        commands = []
        current_cmd = None
        current_args = []
        
        for arg in args:
            if arg.startswith("--"):
                current_args.append(arg)
            elif current_cmd is None:
                current_cmd = arg
            else:
                # Save previous command and start new one
                commands.append({"command": current_cmd, "args": current_args})
                current_cmd = arg
                current_args = []
        
        if current_cmd:
            commands.append({"command": current_cmd, "args": current_args})
        
        if not commands:
            print("Error: No commands provided for pipeline")
            sys.exit(1)
        
        print(f"Running pipeline with {len(commands)} commands...")
        result = mdx2_pipeline_flow(commands)
        print(f"Pipeline completed: {result}")
    
    elif command == "map":
        # Parse map arguments
        if len(args) < 2:
            print("Error: map requires at least geom and hkl files")
            sys.exit(1)
        geom, hkl = args[0], args[1]
        outfile = "map.nxs"
        other_args = args[2:]
        
        # Simple parsing for common options
        kwargs = {}
        i = 0
        while i < len(other_args):
            if other_args[i] == "--outfile" and i + 1 < len(other_args):
                outfile = other_args[i + 1]
                i += 2
            elif other_args[i] == "--signal" and i + 1 < len(other_args):
                kwargs["signal"] = other_args[i + 1]
                i += 2
            elif other_args[i] == "--no-symmetry":
                kwargs["symmetry"] = False
                i += 1
            else:
                i += 1
        
        result = map_flow(geom, hkl, outfile=outfile, **kwargs)
        print(f"Map flow completed: {result}")
    
    elif command == "integrate":
        # Parse integrate arguments
        if len(args) < 2:
            print("Error: integrate requires at least geom and data files")
            sys.exit(1)
        geom, data = args[0], args[1]
        outfile = "integrated.nxs"
        other_args = args[2:]
        
        kwargs = {}
        i = 0
        while i < len(other_args):
            if other_args[i] == "--outfile" and i + 1 < len(other_args):
                outfile = other_args[i + 1]
                i += 2
            elif other_args[i] == "--mask" and i + 1 < len(other_args):
                kwargs["mask"] = other_args[i + 1]
                i += 2
            elif other_args[i] == "--nproc" and i + 1 < len(other_args):
                kwargs["nproc"] = int(other_args[i + 1])
                i += 2
            else:
                i += 1
        
        result = integrate_flow(geom, data, outfile=outfile, **kwargs)
        print(f"Integrate flow completed: {result}")
    
    elif command == "scale":
        # Parse scale arguments
        if len(args) < 1:
            print("Error: scale requires at least hkl file")
            sys.exit(1)
        hkl = args[0]
        outfile = "scaled.nxs"
        other_args = args[1:]
        
        kwargs = {}
        i = 0
        while i < len(other_args):
            if other_args[i] == "--outfile" and i + 1 < len(other_args):
                outfile = other_args[i + 1]
                i += 2
            else:
                i += 1
        
        result = scale_flow(hkl, outfile=outfile, **kwargs)
        print(f"Scale flow completed: {result}")
    
    else:
        # Generic command runner - pass through to mdx2 CLI
        print(f"Running mdx2 command: {command} with args: {args}")
        result = run_mdx2_cli_command(command, args)
        print(f"Command completed: {result}")


if __name__ == "__main__":
    main()
