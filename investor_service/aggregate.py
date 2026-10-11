"""Investor aggregate.

The investor service owns this decision. ``aggregate`` takes the investor row
and one subscription and returns whether that subscription may be accepted.
It does not read Postgres or publish to Kafka. The service loop does those
steps around this result.

On ``InvestorValidated``, ``balance`` is the new ``subscribed_amount`` to
persist. On ``InvestorRejected``, ``balance`` is the unchanged total. On
``ProcessingFailed``, ``balance`` is ``"N/A"`` and must not be written.
"""


def check_commitment_limit(subscription_amount, balance, commitment_amount):
    """Return whether the subscription still fits under the investor's cap."""
    return (subscription_amount + balance) <= commitment_amount


def check_negative_balance(subscription_amount, balance):
    """Return whether the subscription amount is zero or positive.

    ``balance`` is unused. A negative amount is a rejection. Decreasing the
    subscribed total belongs to a later redemption command.
    """
    return subscription_amount >= 0


def check_active_investor(status):
    """Return whether the investor is allowed to subscribe."""
    return status == "active"


def check_investor_currency(event_currency, stored_currency):
    """Return whether the subscription currency matches the investor's."""
    return event_currency == stored_currency


def aggregate(investor, subscription):
    """Apply the investor rules to one subscription.

    Checks run in order, and the first failure is the result:

    1. ``subscribed_amount + subscription_amount`` must not exceed
       ``commitment_amount``.
    2. ``subscription_amount`` must be greater than or equal to zero.
    3. ``status`` must be ``"active"``.
    4. The subscription currency must match the investor currency.

    ``fund_id`` is not checked here. Fund existence and capacity belong to
    the fund aggregate. This function also does not recognize a repeated
    message, so applying the same accepted subscription twice increases the
    balance twice.

    Args:
        investor: Current aggregate row. Required keys are ``subscribed_amount``,
            ``commitment_amount``, ``currency``, and ``status``.
        subscription: One subscription request. Required keys are
            ``subscription_amount`` and ``currency``.

    Returns:
        A dict with ``event_type``, ``message``, and ``balance``.
        ``event_type`` is ``InvestorValidated``, ``InvestorRejected``, or
        ``ProcessingFailed``.
    """
    try:

        balance = investor['subscribed_amount']
        commitment_amount = investor['commitment_amount']
        investor_currency = investor['currency']
        status = investor['status']
        subscription_amount = subscription['subscription_amount']
        subscription_currency = subscription['currency']

        does_not_exceed_commitment = check_commitment_limit(
            subscription_amount, balance, commitment_amount)
        positive_balance = check_negative_balance(subscription_amount, balance)
        active_investor = check_active_investor(status)
        valid_currency = check_investor_currency(
            investor_currency, subscription_currency)

        if not does_not_exceed_commitment:
            return {
                "event_type": "InvestorRejected",
                "message": f"Subscription amount of {subscription_amount} added to balance of {balance} would exceed the investors commitment of {commitment_amount}",
                "balance": balance
            }
        elif not positive_balance:
            return {
                "event_type": "InvestorRejected",
                "message": f"Subscription amount ({subscription_amount}) must be greater than zero",
                "balance": balance
            }
        elif not active_investor:
            return {
                "event_type": "InvestorRejected",
                "message": "Investor status is not listed as active",
                "balance": balance
            }
        elif not valid_currency:
            return {
                "event_type": "InvestorRejected",
                "message": f"Subscription currency ({subscription_currency}) does not match investor currency ({investor_currency})",
                "balance": balance
            }
        return {
            "event_type": "InvestorValidated",
            "message": "Investor passed all validation checks",
            "balance": (balance + subscription_amount)
        }

    except Exception as e:
        return {
            "event_type": "ProcessingFailed",
            "message": f"Processing failed: {e}",
            "balance": "N/A"
        }
