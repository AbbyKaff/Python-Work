import win32com.client

for x,y,z,a,b,c in zip(fulldf['FullName'], fulldf['Store E-mail'], fulldf['E-mail'], fulldf['Date Terminated'], fulldf['ExceptionDescription'],fulldf['test']):
    outlook = win32com.client.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = y
    mail.CC = z
    mail.BCC = c
    mail.Subject = 'Terminated Employee ' + x + ' is punching in Merit'
    mail.HTMLBody = """\
        <html>
            <head></head>
            <body>
                <p>Hello – """+ x +" has a termination date in kronos as of: "+ a +""" and they are actively punching in Merit.<br><br>
                
                Please ensure they are appropriately rehired in people matter with completed onboarding and appear active in kronos.<br><br>
                    
                If there are any concerns or questions please reach out to the HRIS team. <br><br>
                    
                    Thank you,<br>
                    HRIS <br>
                    hris@kbpbrands.com <br><br>
                    *this is an automated report
                </p>
            </body>
        </html>
        """
    #mail.Attachments.Add(r"")
    mail.Send()
