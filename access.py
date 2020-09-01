import base64
import getpass


def getcredentials():
    user = getpass.getpass('UltraDNS API User: ')
    password = getpass.getpass('Password: ')
    user_e = base64.b64encode(user.encode("utf-8"))
    password_e = base64.b64encode(password.encode("utf-8"))

    with open('cred.ini', 'wb') as f:
        f.write(user_e)
        f.write(b'\n')
        f.write(password_e)


def decode(code):
    code = base64.b64decode(code)
    return code


if __name__ == "__main__":
    getcredentials()