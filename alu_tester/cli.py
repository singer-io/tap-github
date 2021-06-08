import io
import inspect
import json
import sys
import unittest.mock
import tempfile

class PatchStdOut():
    __old_std_out_write = sys.stdout.write

    def __init__(self):
        self.out = io.StringIO()
    
    def stdout_dispatcher(self, text):
        # TODO: Is there a better or more standard way to find this?
        # - Other debuggers? Are there any other debuggers?
        pdb_frames = [f.filename for f in inspect.stack() if f.filename.endswith('pdb.py')]
        if pdb_frames:
            self.__old_std_out_write(text)
        else:
            self.out.write(text)
    
    def __enter__(self):
        sys.stdout.write = self.stdout_dispatcher

    def __exit__(self, _tp, _v, _tb):
        sys.stdout.write = self.__old_std_out_write

def __call_entry_point():
    # TODO: This would need to be more resilient
    # - Maybe it just takes an argument with the tap entry-point command or tap module or tap name
    # - I think that'd be best... if two taps are installed, this would break.
    # TODO: Any errors thrown here need to be VERY verbose and actionable

    # Find Main Entry Point through package resources
    entry_map = [a for a in __import__('pkg_resources').working_set if 'tap-' in a.project_name][0].get_entry_map()
    entry_points = [fun for fun in entry_map.get('console_scripts',{}).values()]
    discovered_main = entry_points[0].resolve()
    return discovered_main()

def run_discovery(config):
    # Call it with mocks and temp files to simulate CLI
    patched_io = PatchStdOut()
    with patched_io, \
         tempfile.NamedTemporaryFile(mode='w') as config_file, \
         unittest.mock.patch('sys.argv', ['tap-tester', '--discover', '--config', config_file.name]):
        
        json.dump(config, config_file)
        config_file.flush()
        __call_entry_point()

        # Return result of mocked sys.stdout
        return patched_io.out.getvalue()
