# coding=utf-8
from multiprocessing.pool import ThreadPool
from threading import Lock

from cloudshell.helpers.scripts import cloudshell_scripts_helpers as helpers
from cloudshell.api.common_cloudshell_api import CloudShellAPIError
from cloudshell.core.logger import qs_logger
from sandbox_scripts.profiler.env_profiler import profileit
from sandbox_scripts.helpers.resource_helpers import get_vm_custom_param, get_resources_created_in_res
from cloudshell.api.cloudshell_api import ReservationDescriptionInfo
import time

class EnvironmentTeardown:
    REMOVE_DEPLOYED_RESOURCE_ERROR = 153

    def __init__(self):
        self.reservation_id = helpers.get_reservation_context_details().id
        self.logger = qs_logger.get_qs_logger(log_file_prefix="CloudShell Sandbox Teardown",
                                              log_group=self.reservation_id,
                                              log_category='Teardown')

    @profileit(scriptName="Teardown")
    def execute(self):
        api = helpers.get_api_session()
        reservation_details = api.GetReservationDetails(self.reservation_id)

        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Beginning reservation teardown')

        self._disconnect_all_routes_in_reservation(api, reservation_details)

        self._power_off_and_delete_all_vm_resources(api, reservation_details, self.reservation_id)

        self._cleanup_connectivity(api, self.reservation_id)

        reservation_details = api.GetReservationDetails(self.reservation_id)
        self._reset_devices(api, reservation_details)

        self.logger.info("Teardown for reservation {0} completed".format(self.reservation_id))
        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Reservation teardown finished successfully')


    def _reset_devices(self, api, reservation_details):
        """

        :param api:
        :param reservation_details:  ReservationDescriptionInfo
        :return:
        """

        running_resources = []
        for resource in reservation_details.ReservationDescription.Resources:
            if '/' not in resource.FullAddress:

                command_list = api.GetResourceCommands(resource.Name).Commands

                for command in command_list:
                    if command.Name == 'reset':
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='Resetting ' +
                                                            resource.Name + ' to factory default')
                        try:
                            api.EnqueueCommand(self.reservation_id, resource.Name, 'Resource', 'reset')
                            running_resources.append(resource.Name)
                            time.sleep(2)
                        except Exception as exc:
                            self.logger.error('Error resetting {0} to factory default. Error: {1}'.format(resource.Name,
                                                                                                          str(exc)))
                        break

        time.sleep(30)
        command_completion = False
        loop_count = 0
        running_resources_copy = running_resources
        while not command_completion and loop_count < 120:
            still_running = False
            for resource in running_resources:
                status = api.GetResourceLiveStatus(resource)

                if status.liveStatusName == 'Progress 10':
                    still_running = True
                else:
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                        message='Factory reset complete on ' +
                                                                resource)
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='-- Status: ' +
                                                                                                   status.liveStatusName)
                    self.logger.info(
                        'reset completed on {0}. Status: {1}:{2}'.format(resource, status.liveStatusName,
                                                                                 status.liveStatusDescription))
                    running_resources_copy.remove(resource)
            if not still_running:
                command_completion = True

            loop_count += 1
            time.sleep(10)

        if not command_completion:
            self.logger.error(
                "At least one resource did not complete reset: " + ','.join(running_resources_copy))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message='The following resources failed to complete factory reset: ' +
                                                        ','.join(running_resources_copy))

        running_resources = []
        for resource in reservation_details.ReservationDescription.Resources:
            if '/' not in resource.FullAddress:

                command_list = api.GetResourceCommands(resource.Name).Commands

                for command in command_list:
                    if command.Name == 'restore_device_id':
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                            message='Restoring device id on ' + resource.Name)
                        try:
                            api.EnqueueCommand(self.reservation_id, resource.Name, 'Resource', 'restore_device_id')
                            running_resources.append(resource.Name)
                            time.sleep(2)
                        except Exception as exc:
                            self.logger.error('Error restoring device id on {0}. Error: {1}'.format(resource.Name,
                                                                                                          str(exc)))
                        break

        time.sleep(30)
        command_completion = False
        loop_count = 0
        running_resources_copy = running_resources
        while not command_completion and loop_count < 120:
            still_running = False
            for resource in running_resources:
                status = api.GetResourceLiveStatus(resource)

                if status.liveStatusName == 'Progress 10':
                    still_running = True
                else:
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                        message='Retore device ID complete on ' +
                                                                resource)
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='-- Status: ' +
                                                                                                   status.liveStatusName)
                    self.logger.info(
                        'restore_device_id completed on {0}. Status: {1}:{2}'.format(resource, status.liveStatusName,
                                                                                 status.liveStatusDescription))
                    running_resources_copy.remove(resource)
            if not still_running:
                command_completion = True

            loop_count += 1
            time.sleep(10)

        if not command_completion:
            self.logger.error(
                "At least one resource did not complete restore_device_id: " + ','.join(running_resources_copy))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message='The following resources failed to complete restoring device id: ' +
                                                        ','.join(running_resources_copy))

    def _disconnect_all_routes_in_reservation(self, api, reservation_details):
        connectors = reservation_details.ReservationDescription.Connectors
        endpoints = []
        for endpoint in connectors:
            if endpoint.Target and endpoint.Source:
                endpoints.append(endpoint.Target)
                endpoints.append(endpoint.Source)

        if not endpoints:
            self.logger.info("No routes to disconnect for reservation {0}".format(self.reservation_id))
            return

        try:
            self.logger.info("Executing disconnect routes for reservation {0}".format(self.reservation_id))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message="Disconnecting all apps...")
            api.DisconnectRoutesInReservation(self.reservation_id, endpoints)

        except CloudShellAPIError as cerr:
            if cerr.code != "123":  # ConnectionNotFound error code
                self.logger.error("Error disconnecting all routes in reservation {0}. Error: {1}"
                                  .format(self.reservation_id, str(cerr)))
                api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                    message="Error disconnecting apps. Error: {0}".format(cerr.message))

        except Exception as exc:
            self.logger.error("Error disconnecting all routes in reservation {0}. Error: {1}"
                              .format(self.reservation_id, str(exc)))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message="Error disconnecting apps. Error: {0}".format(exc.message))

    def _power_off_and_delete_all_vm_resources(self, api, reservation_details, reservation_id):
        """
        :param CloudShellAPISession api:
        :param GetReservationDescriptionResponseInfo reservation_details:
        :param str reservation_id:
        :return:
        """
        # filter out resources not created in this reservation
        resources = get_resources_created_in_res(reservation_details=reservation_details,
                                                 reservation_id=reservation_id)

        pool = ThreadPool()
        async_results = []
        lock = Lock()
        message_status = {
            "power_off": False,
            "delete": False
        }

        for resource in resources:
            resource_details = api.GetResourceDetails(resource.Name)
            if resource_details.VmDetails:
                result_obj = pool.apply_async(self._power_off_or_delete_deployed_app,
                                              (api, resource_details, lock, message_status))
                async_results.append(result_obj)

        pool.close()
        pool.join()

        resource_to_delete = []
        for async_result in async_results:
            result = async_result.get()
            if result is not None:
                resource_to_delete.append(result)

        # delete resource - bulk
        if resource_to_delete:
            try:
                api.RemoveResourcesFromReservation(self.reservation_id, resource_to_delete)
            except CloudShellAPIError as exc:
                if exc.code == EnvironmentTeardown.REMOVE_DEPLOYED_RESOURCE_ERROR:
                    self.logger.error(
                            "Error executing RemoveResourcesFromReservation command. Error: {0}".format(exc.message))
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                        message=exc.message)

    def _power_off_or_delete_deployed_app(self, api, resource_info, lock, message_status):
        """
        :param CloudShellAPISession api:
        :param Lock lock:
        :param (dict of str: Boolean) message_status:
        :param ResourceInfo resource_info:
        :return:
        """
        resource_name = resource_info.Name
        try:
            delete = "true"
            auto_delete_param = get_vm_custom_param(resource_info, "auto_delete")
            if auto_delete_param:
                delete = auto_delete_param.Value

            if delete.lower() == "true":
                self.logger.info("Executing 'Delete' on deployed app {0} in reservation {1}"
                                 .format(resource_name, self.reservation_id))

                if not message_status['delete']:
                    with lock:
                        if not message_status['delete']:
                            message_status['delete'] = True
                            if not message_status['power_off']:
                                message_status['power_off'] = True
                                api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                    message='Apps are being powered off and deleted...')
                            else:
                                api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                    message='Apps are being deleted...')

                # removed call to destroy_vm_only from this place because it will be called from
                # the server in RemoveResourcesFromReservation

                return resource_name
            else:
                power_off = "true"
                auto_power_off_param = get_vm_custom_param(resource_info, "auto_power_off")
                if auto_power_off_param:
                    power_off = auto_power_off_param.Value

                if power_off.lower() == "true":
                    self.logger.info("Executing 'Power Off' on deployed app {0} in reservation {1}"
                                     .format(resource_name, self.reservation_id))

                    if not message_status['power_off']:
                        with lock:
                            if not message_status['power_off']:
                                message_status['power_off'] = True
                                api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                    message='Apps are powering off...')

                    api.ExecuteResourceConnectedCommand(self.reservation_id, resource_name, "PowerOff", "power")
                else:
                    self.logger.info("Auto Power Off is disabled for deployed app {0} in reservation {1}"
                                     .format(resource_name, self.reservation_id))
            return None
        except Exception as exc:
            self.logger.error("Error deleting or powering off deployed app {0} in reservation {1}. Error: {2}"
                              .format(resource_name, self.reservation_id, str(exc)))
            return None

    def _cleanup_connectivity(self, api, reservation_id):
        """
        :param CloudShellAPISession api:
        :param str reservation_id:
        :return:
        """
        self.logger.info("Cleaning-up connectivity for reservation {0}".format(self.reservation_id))
        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Cleaning-up connectivity')
        api.CleanupSandboxConnectivity(reservation_id)
