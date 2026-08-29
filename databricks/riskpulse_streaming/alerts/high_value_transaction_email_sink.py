from pyspark import pipelines as dp

# Get Gmail credentials from secrets outside the function to avoid serialization issues
gmail_api_key = dbutils.secrets.get(
    scope="riskpulse-scope",
    key="gmail_api_key"
)
EMAIL = "parthivd00@gmail.com"
APP_PASSWORD = gmail_api_key

@dp.foreach_batch_sink(name="high_value_alert_email_sink")
def send_high_value_alert_emails(df, batch_id):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    alerts = df.collect()
    if not alerts:
        print(f"ℹ️ Batch {batch_id}: No alerts to process")
        return

    print(f"📧 Batch {batch_id}: Processing {len(alerts)} alert(s)")
    for alert in alerts:
        try:
            alert_id = alert.alert_id
            customer_email = alert.customer_email
            customer_name = alert.customer_name
            transaction_amount = alert.transaction_amount
            transaction_limit = alert.transaction_limit
            currency = alert.currency
            merchant_name = alert.merchant_name
            merchant_category = alert.merchant_category
            transaction_timestamp = alert.transaction_timestamp
            transaction_id = alert.transaction_id

            body = f"""
<html>
<body>
<h2>⚠️ High Value Transaction Alert</h2>
<p>Dear {customer_name},</p>
<p>We detected a high-value transaction on your account that exceeds your configured transaction limit.</p>
<h3>Transaction Details:</h3>
<ul>
    <li><b>Alert ID:</b> {alert_id}</li>
    <li><b>Transaction ID:</b> {transaction_id}</li>
    <li><b>Amount:</b> {transaction_amount} {currency}</li>
    <li><b>Your Limit:</b> {transaction_limit} {currency}</li>
    <li><b>Merchant:</b> {merchant_name}</li>
    <li><b>Category:</b> {merchant_category}</li>
    <li><b>Timestamp:</b> {transaction_timestamp}</li>
</ul>
<p><b>If you did not authorize this transaction, please contact us immediately.</b></p>
<br>
<p>
Thanks<br>
<b>RiskPulse Support Team</b>
</p>
</body>
</html>
            """

            subject = f"⚠️ High Value Transaction Alert - {currency} {transaction_amount}"
            msg = MIMEMultipart()
            msg["From"] = EMAIL
            msg["To"] = customer_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html"))

            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(EMAIL, APP_PASSWORD)
                server.send_message(msg)

            print(f"✅ Email sent to {customer_email} for alert {alert_id}")

        except Exception as e:
            print(f"❌ Error sending email for alert {alert_id}: {e}")
            continue

    print(f"✅ Batch {batch_id}: Completed processing {len(alerts)} alert(s)")


@dp.append_flow(target="high_value_alert_email_sink")
def stream_alerts_to_email():
    return spark.readStream.table("riskpulse.gold.high_value_transaction_alert")