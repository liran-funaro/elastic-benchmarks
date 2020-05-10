"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import os
from cloudexp.guest.application import Application
from cloudexp.guest.application.benchmark import BenchmarkError, BenchmarkLoad, BenchmarkDuration, BenchmarkResults, \
    Benchmark

from mom.util.parsers import parse_float, parse_int


class Host_DayTrader:
    def __init__(self, **kwargs):
        HostProgram.__init__(self, ApacheBench, (), **kwargs)
        self.logger = logging.getLogger("DayTrader-%s" % self.alias)

    def get_response(self):
        url = "http://%s:8080/daytrader/" % self.ip
        while True:
            try:
                out = urllib.urlopen(url).read()
                if len(out) > 0 and out.find("HTTP Status 404") == -1:
                    break
            except IOError:
                pass
            time.sleep(1)

    def wait_for_program_init(self):
        self.logger.info("Wait for initialization...")
        self.get_response()

        # reset
        self.logger.info("Reset...")
        reset_url = "http://%s:8080/daytrader/config?action=resetTrade" % self.ip
        urllib.urlopen(reset_url).read()

        # setting options:
        market_summary_interval = 20
        primitive_iterations = 1
        RunTimeMode = "Session (EJB3) To Direct"
        OrderProcessingMode = "Asynchronous_2-Phase"
        AcessMode = "Standard"
        soap_url = "http://%s:8080/daytrader/services/TradeWSServices" % self.ip
        WorkloadMix = "Standard"
        WebInterface = "JSP"
        EnableLongRun = True

        options = {"action": "updateConfig",
                   "RunTimeMode": {"Full EJB3": "0",
                                   "Direct (JDBC)": "1",
                                   "Session (EJB3) To Direct": "2",
                                   "Web JDBC": "3",
                                   "Web JPA": "4"}[RunTimeMode],
                   "OrderProcessingMode": {
                       "Synchronous": "0", "Asynchronous_2-Phase": "1"
                   }[OrderProcessingMode],
                   "AcessMode": {"Standard": "0", "WebServices": "1"
                                 }[AcessMode],
                   "SOAP_URL": soap_url,
                   "WorkloadMix": {"Standard": "0", "High-Volume": "1"
                                   }[WorkloadMix],
                   "WebInterface": {"JSP": "0", "JSP-Images": "1"
                                    }[WebInterface],
                   "MaxUsers": self.max_users,
                   "MaxQuotes": self.max_quotes,
                   "marketSummaryInterval": market_summary_interval,
                   "primIterations": primitive_iterations,
                   "EnableLongRun": EnableLongRun
                   }

        self.logger.info("applying requested settings...")
        data = urllib.urlencode(options)
        urllib.urlopen("http://%s:8080/daytrader/config" % self.ip, data)
        self.logger.info("Check again for response")
        self.get_response()
        self.logger.info("Ready!")


class DayTrader(Application):
    def_heap_size = 2500
    def_max_users = 200
    def_max_quotes = 400

    def __init__(self, heapsize=def_heap_size, max_users=def_max_users, max_quotes=def_max_quotes):
        self.heapsize = int(heapsize)
        self.max_users = int(max_users)
        self.max_quotes = int(max_quotes)
        Application.__init__(self)

    def start_application(self):
        if self.prog is not None:
            return
        os.chdir("/root/geronimo")
        cmd = "/root/geronimo/geronimo-tomcat6-javaee5-2.2/bin/start-server -J" \
              " -Xmx%iM -J -Xms%iM" % (self.heapsize, self.heapsize)

        self.prog = Popen(cmd, shell=True, preexec_fn=os.setsid, stdout=PIPE, stderr=PIPE)
        return self.prog.communicate()

    def terminate_application(self):
        # self.prog.terminate() doesn't work since it must be
        # invoked with shell=True... thus must use pkill java.
        Popen("pkill java", shell=True).communicate()
        self.prog = None


class ApacheBench(Benchmark):
    def __init__(self):
        Benchmark.__init__(self)

    def consume(self, load: BenchmarkLoad, duration: BenchmarkDuration) -> BenchmarkResults:
        dt_address = f"http://{self.application_ip}:8080/daytrader/scenario"
        self.logger.debug("starting: load: %i, executions: %i" % (load, duration))

        args = map(str, ("ab", "-c", load, "-t", duration, dt_address))
        output = self.popen(args, raise_stderr=True)  # blocking

        #        self.logger.debug("\n".join(["=== ab out:", output[0],
        #                                     "=== ab err:", output[1]]))
        res = {'req_sec': parse_float(r"Requests per second:\s+(.*) \[\#/sec\]", output),
               'ms_req': parse_float(r"Time per request:\s+(.*) \[ms\] \(mean\)", output),
               'ms_req_across': parse_float(r"Time per request:\s+(.*) \[ms\] \(mean, across all concurrent requests\)",
                                            output),
               'percent95': parse_int(r"95%\s(.*)", output),
               'test_time': parse_float(r"Time taken for tests:\s+(.*) seconds", output)}
        if any((x is None for x in res.values())):
            self.logger.warn("failed to parse ab output: %s", output)
            raise BenchmarkError("failed to parse ab output: %s" % output)
        else:
            self.logger.debug("Ended with %s", str(res))
        return res
