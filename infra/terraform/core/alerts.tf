# Two alarms, about $0.20 a month: the poller stopped collecting, or the static watcher keeps
# failing. Both email through one SNS topic (confirm the subscription email once).

resource "aws_sns_topic" "alerts" {
  name = "ghostbus-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# The poller logs one Embedded Metric Format line per call: GhostBus/Collected = 1 when the
# snapshot is safe in the archive. Ten minutes without one means the archive has a hole.
resource "aws_cloudwatch_metric_alarm" "not_collecting" {
  alarm_name          = "ghostbus-not-collecting"
  alarm_description   = "No GTFS-R snapshot archived in 10 minutes. Gaps become NO_DATA, never ghosts, but they cost data."
  namespace           = "GhostBus"
  metric_name         = "Collected"
  statistic           = "Sum"
  period              = 600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  actions_enabled     = var.collector_enabled
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "static_watcher_failing" {
  alarm_name          = "ghostbus-static-watcher-failing"
  alarm_description   = "The static GTFS check failed three hours in a row."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.static_watcher.function_name }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  actions_enabled     = var.collector_enabled
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
