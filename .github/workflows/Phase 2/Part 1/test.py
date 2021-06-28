import emails

message = emails.html(html="<p>Hi!<br>Here is your receipt...",
                          subject="Your receipt No. 567098123",
                          mail_from=('poly audit', 'poly_audit@bomisco.com'))

r = message.send(to=('Ishmeet Bindra', 'ibindra1995@gmail.com'),
                 render={'name': 'John'},
                 smtp={'host': 'smtp.zoho.com', 'port': 465, 'ssl': True, 'user': 'poly_audit@bomisco.com', 'password': 'Uw$2H@dY'})
assert r.status_code == 250