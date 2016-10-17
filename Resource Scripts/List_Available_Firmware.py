import cloudshell.helpers.scripts.cloudshell_scripts_helpers as helpers
import ftplib
from cloudshell.api.cloudshell_api import CloudShellAPISession

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
try:
    ftp.retrbinary('retr version_index.txt', version_lines.append)
except Exception as exc:
    ses.WriteMessageToReservationOutput(reservation.id,
                                        'Unable to retrieve list of images from FTP. Error: {0}'.format(str(exc)))

    raise exc

try:
    version_lines = version_lines[0].split('\r')
    version_lines  = map(str.strip, version_lines)
    version_lookup = {}

except Exception as exc:
    ses.WriteMessageToReservationOutput(reservation.id,
                                        'Unable to parse fimware version list. Error: {}'.format(str(exc)))
    raise exc

print '\n========================\nAvailable Versions\n[Version]: [File_Name]\n========================'

for line in version_lines:
    model, version_string, path = line.split(',')
    if model not in version_lookup:
        version_lookup[model] = {}
    version_lookup[model][version_string] = path


for ver, file in version_lookup[resource_model].iteritems():
    print ver + ': ' + file

