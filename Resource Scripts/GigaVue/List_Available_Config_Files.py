import cloudshell.helpers.scripts.cloudshell_scripts_helpers as helpers
import ftplib

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
resource_model = resource.attributes['Model']

remote_host, user, password = get_ftp(ses, reservation)

try:
    ftp = ftplib.FTP(remote_host)
    ftp.login(user,password)

except Exception as exc:
    ses.WriteMessageToReservationOutput(reservation.id,
                                        'Unable to connect to FTP server to retreive list. Error: {0}'.format(str(exc)))
    raise exc

version_lines=[]
device_configs = []
try:
    device_configs = ftp.nlst('Configs/Devices/' + resource.name)

except Exception as exc:
    if exc.message == '550 Directory not found.':
        device_configs = []
    else:
        ses.WriteMessageToReservationOutput(reservation.id,
                                        'Unable to retrieve list of configs from FTP. Error: {0}'.format(str(exc)))

try:
    model_configs = ftp.nlst('Configs/Models/' + resource_model)

except Exception as exc:
    ses.WriteMessageToReservationOutput(reservation.id,
                                        'Unable to retrieve list of configs from FTP. Error: {0}'.format(str(exc)))

    raise exc


print '\n========================\nAvailable Configs Full Path\n========================'
if len(device_configs) > 0:
    print 'For This Device'
    for config in device_configs:
        print '--' + config

print 'For any device of model ' + resource_model
for config in model_configs:
    print '--' + config