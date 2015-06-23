import swiftclient
user = "ExternosExternos:sdclouduser5"
key = "#cloudtrut4sd#"

osOptions = dict()
osOptions["user_domain_name"] = "Externos"
osOptions["project_domain_name"] = "Externos"
osOptions["project_name"] = "SD-Cloud"
osOptions["tenant_name"] = "SD-Cloud"


conn = swiftclient.Connection(
        user=user,
        key=key,
        authurl="http://10.5.0.14:5000/v2.0",
        #insecure=True,
        os_options=osOptions,
        auth_version="2.0",
)

print "Connected"

container_name = "tarciso-container"

with open('simple-producer.py', 'r') as hello_file:
        conn.put_object(container_name, 'simple-producer.py',
                                        contents= hello_file.read(),
                                        content_type='text/plain')

