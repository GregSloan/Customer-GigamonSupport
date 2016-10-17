import cloudshell.helpers.scripts.cloudshell_scripts_helpers as helpers
import os
from cloudshell.api.cloudshell_api import *


def get_ftp(api, reservation):
    """

    :type api: CloudShellAPISession
    :type reservation:  ReservationContextDetails
    :return:
    """
    resv_det = api.GetReservationDetails(reservation.id)

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

ses = helpers.get_api_session()
reservation = helpers.get_reservation_context_details()
resource = helpers.get_resource_context_details()
filename = os.environ['FileName']
filename_input = InputNameValue('file_path', filename)
ftp, user, password = get_ftp(ses, reservation)
remote_host_input = InputNameValue('remote_host', ftp)

ses.EnqueueCommand(reservation.id, resource.name, 'Resource', 'load_firmware', [filename_input, remote_host_input])
