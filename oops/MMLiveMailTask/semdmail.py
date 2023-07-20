import json
import smtplib
from email.message import EmailMessage
import os

file_path = os.path.join(os.path.dirname(__file__), 'backendcred.json')
dlog_cred = json.load(open(file_path))

mail_path = os.path.join(os.path.dirname(__file__), 'mailformat.json')
mail_format = json.load(open(mail_path))

sf_username = dlog_cred.get('EMAIL_CRED').get('EMAIL_USERNAME')
sf_password = dlog_cred.get('EMAIL_CRED').get('EMAIL_PASSWORD')
sf_ehost = dlog_cred.get('EMAIL_CRED').get('EMAIL_HOST')
sf_eport = dlog_cred.get('EMAIL_CRED').get('EMAIL_PORT')
msg = EmailMessage()


class SendEmail:
    sf_signature = """<p>{body}</p>
    <hr style='border-width:0;height:1px;color:#404040;background-color:#404040;'>
    <p style="color:#404040"> Thank you, <br> {org}</p>"""

    def __init__(self, to, cc=""):
        del msg['From']
        del msg['To']
        del msg['Subject']
        del msg['Cc']
        msg['From'] = sf_username
        msg['To'] = to
        msg['Cc'] = cc

    # Request to Change Password
    @staticmethod
    def mm_req_otp(tenant_id, emailid, otp, org_name, HMT):
        subbody = mail_format.get('SFactoryEmailFormat').get('RqChangePwd')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(emailid, otp), org=org_name),
                        subtype='html')

    # Successfully changed password
    @staticmethod
    def mm_req_changepwd(tenant_id, emailid, domain, org_name, HMT):
        subbody = mail_format.get('SFactoryEmailFormat').get('SucChangedPwd')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(emailid, domain), org=org_name),
                        subtype='html')

    # Update Profile
    @staticmethod
    def mm_updatepro(tenant_id, emailid, domain, org_name, HMT):
        subbody = mail_format.get('SFactoryEmailFormat').get('ProfileUpdate')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(emailid, domain), org=org_name),
                        subtype='html')

    # Added User
    @staticmethod
    def mm_addeduser(tenant_id, by_emailid, role, HMT, domain, emailid, password, org_name):
        print('11')
        subbody = mail_format.get('SFactoryEmailFormat').get('NewUserAdded')
        print('12')
        msg['Subject'] = subbody[0]
        print('13')
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(by_emailid, role,HMT, domain, emailid, password),
                                          org=org_name), subtype='html')
        print('90')

    # Updated User
    @staticmethod
    def mm_updateduser(tenant_id, by_emailid, role, domain, org_name):
        subbody = mail_format.get('SFactoryEmailFormat').get('NewUserUpdated')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(by_emailid, role, domain), org=org_name),
                        subtype='html')

    # Deleted User
    @staticmethod
    def mm_deleteduser(tenant_id, by_emailid, org_name):
        subbody = mail_format.get('SFactoryEmailFormat').get('NewUserDeleted')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(by_emailid), org=org_name), subtype='html')

    # New Registration
    @staticmethod
    def mm_new_register(HMT, link):
        subbody = mail_format.get('SFactoryEmailFormat').get('NewRegister')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1].format(HMT, link), org='Machine_Monitoring'), subtype='html')

    # Successfully Registered
    @staticmethod
    def mm_updateCred(tenant_id, org_name):
        subbody = mail_format.get('SFactoryEmailFormat').get('SucRegister')
        msg['Subject'] = subbody[0]
        msg.set_content(SendEmail.sf_signature.format(body=subbody[1], org=org_name), subtype='html')


    @staticmethod
    def send():
        smtp = smtplib.SMTP(host=sf_ehost, port=sf_eport)
        print('91')
        smtp.starttls()
        print('92')
        smtp.login(sf_username, sf_password)
        print('93')
        smtp.send_message(msg)
        print('97')
        smtp.quit()
