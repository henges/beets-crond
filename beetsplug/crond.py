from beets.plugins import BeetsPlugin, commands
from beets.ui import Subcommand
from optparse import Values
from cron_converter import Cron
import sched, time

crond_command = Subcommand('crond', help='run a command on a schedule')
crond_command.parser.add_option("-r", "--run", help="The command to run, including all options.", default=None)
crond_command.parser.add_option("-c", "--cron", help="The cron to use to execute the command.", default=None)

class BeetsCrond(BeetsPlugin):
    cmddict = {}
    def __init__(self):
        super(BeetsCrond, self).__init__()
        self.register_listener('pluginload', self.set_command_dict)

    def set_command_dict(self):
        from beets.ui.commands import default_commands
        self.cmddict = {c.name: c for c in default_commands + commands()}

    def commands(self):
        # Bind this instance to the register function
        # so that we can access our config
        def curried(lib, opts, args):
            return self.register(lib, opts, args)
        
        crond_command.func = curried

        return [crond_command]

    def register(self, lib, opts: Values, args):
        print("opts: {opts}".format(opts=opts))
        print("config: {config}".format(config=self.config))
        raw_run = (opts.run or self.config["run"].as_str()).split(" ", 1)
        if len(raw_run) > 1:
            [run, run_args] = raw_run
            run_args = run_args.split(" ")
        else:
            [run] = raw_run
            run_args = []

        cron = (opts.cron or self.config["cron"].as_str())
        if run is None:
            print("No command to run provided.")
            return
        if cron is None:
            print("No schedule definition provided.")
            return

        print("Run: {run}, args: {args}, cron: {cron}".format(run=run, args=run_args, cron=cron))

        ucommand = self.cmddict.get(run)
        if ucommand is None:
            print("Command {cmd} doesn't exist.".format(cmd=run))
            return

        try: 
            ucron = Cron(cron)
        except Exception as e:
            print("Error parsing cron: {e}".format(e=e))
            return
        
        def invoke_cmd():
            subopts, subargs = ucommand.parser.parse_args(run_args)
            ucommand.func(lib, subopts, subargs)
            return

        first = ucron.schedule().start_time.timestamp()
        print("Current time: {t}".format(t=time.time()))
        print("Next: {n}".format(n=first))
        s = sched.scheduler(time.monotonic, time.sleep)

        def action():
            if not s.empty():
                print("Not running '{run} {run_args}' at {time} because an execution is still in progress".format(run=run,run_args=run_args,time=time.time()))
            else:
                print("Running '{run} {run_args}' {time}".format(run=run,run_args=run_args,time=time.time()))
                invoke_cmd()

            next_run = ucron.schedule().start_time.timestamp() - time.time()
            print("Next run in {time} seconds".format(time=next_run))
            s.enter(next_run, 1, action)

        s.enter(first - time.time(), 1, action)
        s.run()

