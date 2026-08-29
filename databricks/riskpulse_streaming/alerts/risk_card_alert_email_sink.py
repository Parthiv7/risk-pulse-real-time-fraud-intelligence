# from pyspark import pipelines as dp

gmail_api_key = dbutils.secrets.get(
    scope="riskpulse-scope",
    key="gmail_api_key"
)
EMAIL = "parthivd00@gmail.com"
APP_PASSWORD = gmail_api_key

from pyspark import pipelines as dp

@dp.foreach_batch_sink(name="risk_card_alert_email_sink")
def send_risk_card_alert_emails(df, batch_id):
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
            watchlist_id = alert.watchlist_id
            risk_level = alert.risk_level
            watch_type = alert.watch_type
            action = alert.action
            reason_code = alert.reason_code
            reason_description = alert.reason_description
            transaction_id = alert.transaction_id
            card_number = alert.card_number
            masked_card = f"****-****-****-{card_number[-4:]}" if len(card_number) >= 4 else "****"
            transaction_amount = alert.amount
            currency = alert.currency
            merchant_name = alert.merchant_name
            merchant_category = alert.merchant_category
            transaction_city = alert.transaction_city
            transaction_country = alert.transaction_country
            transaction_timestamp = alert.transaction_timestamp

            body = f"""
<html>
<body>
<h2>🚨 Fraud Alert - Risk Card Detected</h2>
<p>Dear {customer_name},</p>
<p>We detected a transaction on a card that appears on our risk watchlist. This transaction has been flagged for your immediate attention.</p>
<h3>Alert Details:</h3>
<ul>
    <li><b>Alert ID:</b> {alert_id}</li>
    <li><b>Watchlist ID:</b> {watchlist_id}</li>
    <li><b>Risk Level:</b> {risk_level}</li>
    <li><b>Watch Type:</b> {watch_type}</li>
    <li><b>Action Taken:</b> {action}</li>
    <li><b>Reason Code:</b> {reason_code}</li>
    <li><b>Reason:</b> {reason_description}</li>
</ul>
<h3>Transaction Details:</h3>
<ul>
    <li><b>Transaction ID:</b> {transaction_id}</li>
    <li><b>Card Number:</b> {masked_card}</li>
    <li><b>Amount:</b> {transaction_amount} {currency}</li>
    <li><b>Merchant:</b> {merchant_name}</li>
    <li><b>Category:</b> {merchant_category}</li>
    <li><b>Location:</b> {transaction_city}, {transaction_country}</li>
    <li><b>Timestamp:</b> {transaction_timestamp}</li>
</ul>
<p><b>⚠️ URGENT: If you did not authorize this transaction, please contact us immediately and report your card as compromised.</b></p>
<br>
<p>
Thanks<br>
<b>RiskPulse Fraud Prevention Team</b>
</p>
</body>
</html>
            """

            subject = f"🚨 Fraud Alert - Risk Card Match Detected - {masked_card}"

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


@dp.append_flow(target="risk_card_alert_email_sink")
def stream_risk_card_alerts_to_email():
    return spark.readStream.table("riskpulse.gold.risk_card_alert")