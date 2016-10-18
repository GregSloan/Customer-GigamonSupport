from multiprocessing.pool import ThreadPool
from threading import Lock

from cloudshell.helpers.scripts import cloudshell_scripts_helpers as helpers
from cloudshell.api.cloudshell_api import *
from cloudshell.api.common_cloudshell_api import CloudShellAPIError
from cloudshell.core.logger import qs_logger

from sandbox_scripts.helpers.vm_details_helper import get_vm_custom_param, get_vm_details
from sandbox_scripts.profiler.env_profiler import profileit
import time
import ftplib

class EnvironmentSetup(object):
    NO_DRIVER_ERR = "129"
    DRIVER_FUNCTION_ERROR = "151"

    # HD_GigaVueVersions={'4.5':'hdccv2_2016-03-04_gm.img','4.6':'hdccv2_2016-05-19.img','4.7':'hdccv2_2016-09-08_gm.img'}
    # HC_GigaVueVersions={'4.5':'hc2_2016-03-04_gm.img','4.6':'hc2_2016-05-19.img','4.7':'hc2_2016-09-08_gm.img'}


    def _get_ftp(self, api, reservationId):
        """

        :type api: CloudShellAPISession
        :type reservation:  ReservationContextDetails
        :return:
        """
        resv_det = api.GetReservationDetails(reservationId)

        server = None
        user = None
        password = None
        for resource in resv_det.ReservationDescription.Resources:
            if resource.ResourceModelName.lower() == 'generic tftp server':
                server = resource.FullAddress
                res_det = api.GetResourceDetails(resource.Name)
                for attribute in res_det.ResourceAttributes:
                    if attribute.Name == 'Storage username':
                        user = attribute.Value
                    if attribute.Name == 'Storage password':
                        password = attribute.Value

        return server, user, password

    def __init__(self):
        self.reservation_id = helpers.get_reservation_context_details().id
        self.logger = qs_logger.get_qs_logger(log_file_prefix="CloudShell Sandbox Setup",
                                              log_group=self.reservation_id,
                                              log_category='Setup')

    @profileit(scriptName='Setup')
    def execute(self):
        api = helpers.get_api_session()
        resource_details_cache = {}

        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Beginning reservation setup')

        reservation_details = api.GetReservationDetails(self.reservation_id)

        deploy_result = self._deploy_apps_in_reservation(api=api,
                                                         reservation_details=reservation_details)

        # refresh reservation_details after app deployment if any deployed apps
        if deploy_result and deploy_result.ResultItems:
            reservation_details = api.GetReservationDetails(self.reservation_id)

        self._try_exeucte_autoload(api=api,
                                   reservation_details=reservation_details,
                                   deploy_result=deploy_result,
                                   resource_details_cache=resource_details_cache)

        self._connect_all_routes_in_reservation(api=api,
                                                reservation_details=reservation_details)

        self._run_async_power_on_refresh_ip_install(api=api,
                                                    reservation_details=reservation_details,
                                                    deploy_results=deploy_result,
                                                    resource_details_cache=resource_details_cache)

        remote_host, user, password = self._get_ftp(api, self.reservation_id)

        global_inputs = helpers.get_reservation_context_details().parameters.global_inputs

        api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message = str(global_inputs))

        # Begin Firmware load for IntlTAC environments
        #############################################

        # Check for presense of version selector input
        if 'GigaVue Version' in global_inputs:
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message = 'Beginning load_firmware')
            version = global_inputs['GigaVue Version']
            self._apply_software_image(api=api,
                                   reservation_details=reservation_details,
                                   deploy_result=deploy_result,
                                   resource_details_cache=resource_details_cache,
                                       version=version,
                                    remote_host=remote_host)

        self.logger.info("Setup for reservation {0} completed".format(self.reservation_id))


        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Reservation setup finished successfully')


    def _apply_software_image(self, api, reservation_details, deploy_result, resource_details_cache, version,
                              remote_host):
        """

        :param CloudShellAPISession api:
        :param GetReservationDescriptionResponseInfo reservation_details:
        :param BulkAppDeploymentyInfo deploy_result:
        :param resource_details_cache:
        :return:
        """

        # Reading version lookup info from FTP
        ftp_host, user, password = self._get_ftp(api, self.reservation_id)
        try:
            ftp = ftplib.FTP(ftp_host)  # connect to FTP
            ftp.login(user, password)
        except Exception as exc:
            self.logger.error('Unable to apply software images, unable to connect to FTP server. Error: {0}'.format(str(exc)))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                message='Unable to apply software images, unable to connect to FTP server. Error: {0}'.format(str(exc)))
            return

        version_lines=[]
        try:
            ftp.retrbinary('retr version_index.txt',version_lines.append)  # read version_index.txt
        except Exception as exc:
            self.logger.error('Unable to apply software images, unable to retrieve firmware version file. Error: {0}'.format(str(exc)))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                message='Unable to apply software images, unable to retrieve firmware version file Error: {0}'.format(str(exc)))
            return

        # Parse version_index.txt text into dictionary lookup
        try:
            version_lines=version_lines[0].split('\r')
            version_lines=map(str.strip,version_lines)
            version_lookup = {}
        except:
            self.logger.error('Unable to apply software images, unable to parse firmware version file. Error: {0}'.format(str(exc)))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                message='Unable to apply software images, unable to parse firmware version file Error: {0}'.format(str(exc)))
            return

        for line in version_lines:
            model,version_string,path = line.split(',')
            if model not in version_lookup:
                version_lookup[model] = {}
            version_lookup[model][version_string] = path

        running_resources = []
        for resource in reservation_details.ReservationDescription.Resources: # go through the list of resources
            if '/' not in resource.FullAddress: # this filters out any sub-resources (sub resources have '/' in full add.
                attributes = api.GetResourceDetails(resource.Name).ResourceAttributes
                model=None
                for attribute in attributes:
                    if attribute.Name == 'Model':
                        model = attribute.Value

                command_list = api.GetResourceCommands(resource.Name).Commands

                for command in command_list:
                    if command.Name == 'load_firmware':
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='Loading firmware on ' +
                                                                                               resource.Name)
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='-- ' +
                                                                                               version_lookup[model][version])
                        command_inputs = []
                        command_inputs.append(InputNameValue('file_path', version_lookup[model][version]))
                        command_inputs.append(InputNameValue('remote_host', remote_host))
                        try:
                            api.EnqueueCommand(self.reservation_id, resource.Name, 'Resource', 'load_firmware', command_inputs) # execute it
                            running_resources.append(resource.Name)
                            time.sleep(2)  # Workaround for 'resource temporarily unavailable' error
                        except Exception as exc:
                            self.logger.error("Error executing load_firmware command on resource {0}. Error: {1}"
                                              .format(resource.Name, str(exc)))
                            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                                message='load_firmware failed on "{0}": {1}'
                                                                .format(resource.Name, exc.message))

                        break
        time.sleep(30)  # Wait for command execution to start

        # Wait for executions to complete
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
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='Loading firmware complete on ' +
                                                                                               resource)
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='-- Status: ' +
                                                                                               status.liveStatusName)
                    self.logger.info('load_firmware completed on {0}. Status: {1}:{2}'.format(resource, status.liveStatusName,
                                                                                              status.liveStatusDescription))
                    running_resources_copy.remove(resource)
            if not still_running:
                command_completion = True

            loop_count += 1
            time.sleep(10)

        if not command_completion:
            self.logger.error("At least one resource did not complete load_firmware: " + ','.join(running_resources_copy))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message = 'The following resources failed to complete load_firmware: ' + ','.join(running_resources_copy))


    def _try_exeucte_autoload(self, api, reservation_details, deploy_result, resource_details_cache):
        """
        :param GetReservationDescriptionResponseInfo reservation_details:
        :param CloudShellAPISession api:
        :param BulkAppDeploymentyInfo deploy_result:
        :param (dict of str: ResourceInfo) resource_details_cache:
        :return:
        """

        if deploy_result is None:
            self.logger.info("No apps to discover")
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id, message='No apps to discover')
            return

        message_written = False

        for deployed_app in deploy_result.ResultItems:
            if not deployed_app.Success:
                continue
            deployed_app_name = deployed_app.AppDeploymentyInfo.LogicalResourceName

            resource_details = api.GetResourceDetails(deployed_app_name)
            resource_details_cache[deployed_app_name] = resource_details

            autoload = "true"
            autoload_param = get_vm_custom_param(resource_details, "autoload")
            if autoload_param:
                autoload = autoload_param.Value
            if autoload.lower() != "true":
                self.logger.info("Apps discovery is disabled on deployed app {0}".format(deployed_app_name))
                continue

            try:
                self.logger.info("Executing Autoload command on deployed app {0}".format(deployed_app_name))
                if not message_written:
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                        message='Apps are being discovered...')
                    message_written = True

                api.AutoLoad(deployed_app_name)

            except CloudShellAPIError as exc:
                if exc.code not in (EnvironmentSetup.NO_DRIVER_ERR, EnvironmentSetup.DRIVER_FUNCTION_ERROR):
                    self.logger.error(
                        "Error executing Autoload command on deployed app {0}. Error: {1}".format(deployed_app_name,
                                                                                                  exc.rawxml))
                    api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                        message='Discovery failed on "{0}": {1}'
                                                        .format(deployed_app_name, exc.message))

            except Exception as exc:
                self.logger.error("Error executing Autoload command on deployed app {0}. Error: {1}"
                                  .format(deployed_app_name, str(exc)))
                api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                    message='Discovery failed on "{0}": {1}'
                                                    .format(deployed_app_name, exc.message))

    def _deploy_apps_in_reservation(self, api, reservation_details):
        apps = reservation_details.ReservationDescription.Apps
        if not apps or (len(apps) == 1 and not apps[0].Name):
            self.logger.info("No apps found in reservation {0}".format(self.reservation_id))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message='No apps to deploy')
            return None

        app_names = map(lambda x: x.Name, apps)
        app_inputs = map(lambda x: DeployAppInput(x.Name, "Name", x.Name), apps)

        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Apps deployment started')
        self.logger.info(
            "Deploying apps for reservation {0}. App names: {1}".format(reservation_details, ", ".join(app_names)))

        res = api.DeployAppToCloudProviderBulk(self.reservation_id, app_names, app_inputs)

        return res

    def _connect_all_routes_in_reservation(self, api, reservation_details):
        connectors = reservation_details.ReservationDescription.Connectors
        endpoints = []
        for endpoint in connectors:
            if endpoint.State in ['Disconnected', 'PartiallyConnected', 'ConnectionFailed'] \
                    and endpoint.Target and endpoint.Source:
                endpoints.append(endpoint.Target)
                endpoints.append(endpoint.Source)

        if not endpoints:
            self.logger.info("No routes to connect for reservation {0}".format(self.reservation_id))
            api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                message='Nothing to connect')
            return

        self.logger.info("Executing connect routes for reservation {0}".format(self.reservation_id))
        self.logger.debug("Connecting: {0}".format(",".join(endpoints)))
        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                            message='Connecting all apps')
        res = api.ConnectRoutesInReservation(self.reservation_id, endpoints, 'bi')
        return res

    def _run_async_power_on_refresh_ip_install(self, api, reservation_details, deploy_results, resource_details_cache):
        """
        :param CloudShellAPISession api:
        :param GetReservationDescriptionResponseInfo reservation_details:
        :param BulkAppDeploymentyInfo deploy_results:
        :param (dict of str: ResourceInfo) resource_details_cache:
        :return:
        """
        resources = reservation_details.ReservationDescription.Resources
        if len(resources) == 0:
            api.WriteMessageToReservationOutput(
                reservationId=self.reservation_id,
                message='No resources to power on or install')
            self._validate_all_apps_deployed(deploy_results)
            return

        pool = ThreadPool(len(resources))
        lock = Lock()
        message_status = {
            "power_on": False,
            "wait_for_ip": False,
            "install": False
        }

        async_results = [pool.apply_async(self._power_on_refresh_ip_install,
                                          (api, lock, message_status, resource, deploy_results, resource_details_cache))
                         for resource in resources]

        pool.close()
        pool.join()

        for async_result in async_results:
            res = async_result.get()
            if not res[0]:
                raise Exception("Reservation is Active with Errors - " + res[1])

        self._validate_all_apps_deployed(deploy_results)

    def _validate_all_apps_deployed(self, deploy_results):
        if deploy_results is not None:
            for deploy_res in deploy_results.ResultItems:
                if not deploy_res.Success:
                    raise Exception("Reservation is Active with Errors - " + deploy_res.Error)

    def _power_on_refresh_ip_install(self, api, lock, message_status, resource, deploy_result, resource_details_cache):
        """
        :param CloudShellAPISession api:
        :param Lock lock:
        :param (dict of str: Boolean) message_status:
        :param ReservedResourceInfo resource:
        :param BulkAppDeploymentyInfo deploy_result:
        :param (dict of str: ResourceInfo) resource_details_cache:
        :return:
        """

        deployed_app_name = resource.Name
        deployed_app_data = None

        power_on = "true"
        wait_for_ip = "true"

        try:
            self.logger.debug("Getting resource details for resource {0} in reservation {1}"
                              .format(deployed_app_name, self.reservation_id))

            if deployed_app_name in resource_details_cache:
                resource_details = resource_details_cache[deployed_app_name]
            else:
                resource_details = api.GetResourceDetails(deployed_app_name)

            # check if deployed app
            vm_details = get_vm_details(resource_details)
            if not hasattr(vm_details, "UID"):
                self.logger.debug("Resource {0} is not a deployed app, nothing to do with it".format(deployed_app_name))
                return True, ""

            auto_power_on_param = get_vm_custom_param(resource_details, "auto_power_on")
            if auto_power_on_param:
                power_on = auto_power_on_param.Value

            wait_for_ip_param = get_vm_custom_param(resource_details, "wait_for_ip")
            if wait_for_ip_param:
                wait_for_ip = wait_for_ip_param.Value

            # check if we have deployment data
            if deploy_result is not None:
                for data in deploy_result.ResultItems:
                    if data.Success and data.AppDeploymentyInfo.LogicalResourceName == deployed_app_name:
                        deployed_app_data = data
        except Exception as exc:
            self.logger.error("Error getting resource details for deployed app {0} in reservation {1}. "
                              "Will use default settings. Error: {2}".format(deployed_app_name,
                                                                             self.reservation_id,
                                                                             str(exc)))

        try:
            self._power_on(api, deployed_app_name, power_on, lock, message_status)
        except Exception as exc:
            self.logger.error("Error powering on deployed app {0} in reservation {1}. Error: {2}"
                              .format(deployed_app_name, self.reservation_id, str(exc)))
            return False, "Error powering on deployed app {0}".format(deployed_app_name)

        try:
            self._wait_for_ip(api, deployed_app_name, wait_for_ip, lock, message_status)
        except Exception as exc:
            self.logger.error("Error refreshing IP on deployed app {0} in reservation {1}. Error: {2}"
                              .format(deployed_app_name, self.reservation_id, str(exc)))
            return False, "Error refreshing IP deployed app {0}. Error: {1}".format(deployed_app_name, exc.message)

        try:
            self._install(api, deployed_app_data, deployed_app_name, lock, message_status)
        except Exception as exc:
            self.logger.error("Error installing deployed app {0} in reservation {1}. Error: {2}"
                              .format(deployed_app_name, self.reservation_id, str(exc)))
            return False, "Error installing deployed app {0}. Error: {1}".format(deployed_app_name, str(exc))

        return True, ""

    def _install(self, api, deployed_app_data, deployed_app_name, lock, message_status):
        installation_info = None
        if deployed_app_data:
            installation_info = deployed_app_data.AppInstallationInfo
        else:
            self.logger.info("Cant execute installation script for deployed app {0} - No deployment data"
                             .format(deployed_app_name))
            return

        if installation_info and hasattr(installation_info, "ScriptCommandName"):
            self.logger.info("Executing installation script {0} on deployed app {1} in reservation {2}"
                             .format(installation_info.ScriptCommandName, deployed_app_name, self.reservation_id))

            if not message_status['install']:
                with lock:
                    if not message_status['install']:
                        message_status['install'] = True
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                            message='Apps are installing...')

            script_inputs = []
            for installation_script_input in installation_info.ScriptInputs:
                script_inputs.append(
                    InputNameValue(installation_script_input.Name, installation_script_input.Value))

            installation_result = api.InstallApp(self.reservation_id, deployed_app_name,
                                                 installation_info.ScriptCommandName, script_inputs)

            self.logger.debug("Installation_result: " + installation_result.Output)

    def _wait_for_ip(self, api, deployed_app_name, wait_for_ip, lock, message_status):
        if wait_for_ip.lower() == "true":

            if not message_status['wait_for_ip']:
                with lock:
                    if not message_status['wait_for_ip']:
                        message_status['wait_for_ip'] = True
                        api.WriteMessageToReservationOutput(
                            reservationId=self.reservation_id,
                            message='Waiting for apps IP addresses, this may take a while...')

            self.logger.info("Executing 'Refresh IP' on deployed app {0} in reservation {1}"
                             .format(deployed_app_name, self.reservation_id))

            api.ExecuteResourceConnectedCommand(self.reservation_id, deployed_app_name,
                                                "remote_refresh_ip",
                                                "remote_connectivity")
        else:
            self.logger.info("Wait For IP is off for deployed app {0} in reservation {1}"
                             .format(deployed_app_name, self.reservation_id))

    def _power_on(self, api, deployed_app_name, power_on, lock, message_status):
        if power_on.lower() == "true":
            self.logger.info("Executing 'Power On' on deployed app {0} in reservation {1}"
                             .format(deployed_app_name, self.reservation_id))

            if not message_status['power_on']:
                with lock:
                    if not message_status['power_on']:
                        message_status['power_on'] = True
                        api.WriteMessageToReservationOutput(reservationId=self.reservation_id,
                                                            message='Apps are powering on...')

            api.ExecuteResourceConnectedCommand(self.reservation_id, deployed_app_name, "PowerOn", "power")
        else:
            self.logger.info("Auto Power On is off for deployed app {0} in reservation {1}"
                             .format(deployed_app_name, self.reservation_id))
