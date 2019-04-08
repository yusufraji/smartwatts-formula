# Copyright (C) 2018  INRIA
# Copyright (C) 2018  University of Lille
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from collections import OrderedDict, defaultdict
from datetime import datetime
from enum import Enum
from math import ldexp, fabs
from typing import Dict

from powerapi.handler import Handler
from powerapi.message import UnknowMessageTypeException
from powerapi.report import HWPCReport, PowerReport

from smartwatts.formula import SmartWattsFormula, PowerModelNotInitializedException


class FormulaScope(Enum):
    """
    Enum used to set the scope of the formula.
    """
    CPU = "cpu"
    DRAM = "dram"


class ReportHandler(Handler):
    """
    This reports handler process the HWPC reports to compute a per-target power estimation.
    """

    def __init__(self, sensor: str, pusher, socket: str, scope: FormulaScope, rapl_event: str, error_threshold: float):
        self.sensor = sensor
        self.pusher = pusher
        self.socket = socket
        self.scope = scope
        self.rapl_event = rapl_event
        self.error_threshold = error_threshold
        self.ticks = OrderedDict()
        self.formula = SmartWattsFormula()

    def _gen_rapl_events_group(self, system_report: HWPCReport) -> Dict[str, float]:
        """
        Generate an events group with the RAPL reference event converted in Watts for the current socket.
        :param system_report: The HWPC report of the System target
        :return: A dictionnary containing the RAPL reference event with its value converted in Watts
        """
        cpu_events = next(iter(system_report.groups['rapl'][self.socket].values()))
        energy = ldexp(cpu_events[self.rapl_event], -32)
        return {self.rapl_event: energy}

    def _gen_pcu_events_group(self, system_report: HWPCReport) -> Dict[str, int]:
        """
        Generate an events group with PCU events for the current socket.
        :param system_report: The HWPC report of the System target
        :return: A dictionary containing the PCU events of the current socket
        """
        pcu_events_group = {}
        cpu_events = next(iter(system_report.groups['pcu'][self.socket].values()))
        cpu_events = {k: v for k, v in cpu_events.items() if not k.startswith('time_')}
        for event_name, event_value in cpu_events.items():
            pcu_events_group[event_name] = event_value

        return pcu_events_group

    def _gen_core_events_group(self, report: HWPCReport) -> Dict[str, int]:
        """
        Generate an events group with Core events for the current socket.
        The events value are the sum of the value for each CPU.
        :param report: The HWPC report of any target
        :return: A dictionary containing the Core events of the current socket
        """
        core_events_group = defaultdict(int)
        for _, cpu_events in report.groups['core'][self.socket].items():
            for event_name, event_value in {k: v for k, v in cpu_events.items() if not k.startswith('time_')}.items():
                core_events_group[event_name] += event_value

        return core_events_group

    def _gen_agg_core_report_from_running_targets(self, targets_report: Dict[str, HWPCReport]) -> Dict[str, int]:
        """
        Generate an aggregate Core events group of the running targets for the current socket.
        :param targets_report: List of Core events group of the running targets
        :return: A dictionary containing an aggregate of the Core events for the running targets of the current socket
        """
        agg_core_events_group = defaultdict(int)
        for _, target_report in targets_report.items():
            for event_name, event_value in self._gen_core_events_group(target_report).items():
                agg_core_events_group[event_name] += event_value

        return agg_core_events_group

    def _gen_power_report(self, timestamp: datetime, target: str, formula: str, power: float):
        """
        Generate a power report with the given parameters.
        :param timestamp: Timestamp of the measurements
        :param target: Target name
        :param formula: Formula identifier
        :param power: Power estimation
        :return: A PowerReport filled with the given parameters
        """
        metadata = {'scope': self.scope.value, 'socket': self.socket, 'formula': formula}
        return PowerReport(timestamp, self.sensor, target, power, metadata)

    def _process_oldest_tick(self):
        """
        Process the oldest tick stored in the stack and generate power reports for the running target(s).
        :return: Power reports of the running target(s)
        """
        timestamp, hwpc_reports = self.ticks.popitem(last=False)

        # power reports of the running targets
        power_reports = []

        # prepare required events group of Global target
        global_report = hwpc_reports.pop('all')
        rapl = self._gen_rapl_events_group(global_report)
        pcu = self._gen_pcu_events_group(global_report)
        global_core = self._gen_agg_core_report_from_running_targets(hwpc_reports)

        # fetch power model to use
        model = self.formula.get_power_model(global_core)

        # compute RAPL power report
        rapl_power = rapl[self.rapl_event]
        power_reports.append(self._gen_power_report(timestamp, 'rapl', self.rapl_event, rapl_power))

        # compute Global target power report
        try:
            system_power = model.compute_power_estimation(rapl, pcu, global_core, global_core)
            power_reports.append(self._gen_power_report(timestamp, 'global', model.hash, system_power))
        except PowerModelNotInitializedException:
            return power_reports

        # compute per-target power report
        for target_name, target_report in hwpc_reports.items():
            target_core = self._gen_core_events_group(target_report)
            target_power = model.compute_power_estimation(rapl, pcu, global_core, target_core)
            power_reports.append(self._gen_power_report(timestamp, target_name, model.hash, target_power))

        # store Global report if the power model error exceeds the error threshold
        if fabs(rapl_power - system_power) > self.error_threshold:
            model.store(rapl, pcu, global_core)

        return power_reports

    def _process_report(self, report):
        """
        Process the received report and trigger the processing of the old ticks.
        :param report: HWPC report of a target
        :return: Nothing
        """

        # store the received report into the tick's bucket
        self.ticks.setdefault(report.timestamp, {}).update({report.target: report})

        # start to process the oldest tick only after receiving at least 5 ticks
        if len(self.ticks) > 5:
            return self._process_oldest_tick()

        return []

    def handle(self, msg, state):
        """
        Process a report and send the result(s) to a pusher actor.
        :param msg: Received message
        :param state: Current actor state
        :return: New actor state
        :raise: UnknowMessageTypeException when the given message is not an HWPCReport
        """
        if not isinstance(msg, HWPCReport):
            raise UnknowMessageTypeException(type(msg))

        result = self._process_report(msg)
        for report in result:
            self.pusher.send_data(report)

        return state