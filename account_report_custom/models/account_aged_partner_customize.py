# -*- coding: utf-8 -*-

from odoo import models, api, _, fields
from odoo.tools.misc import formatLang, format_date

import logging

_logger = logging.getLogger(__name__)


class ReportAccountAgedPartnerCustomize(models.AbstractModel):
    _inherit = "account.aged.partner"

    _auto = False
    total_line = True

    order_no = fields.Char(group_operator='max')
    currency_rate = fields.Float(group_operator='max')

    amount_paid = fields.Monetary(currency_field='currency_id')
    amount_residual = fields.Monetary(currency_field='currency_id')
    amount_total_hkd = fields.Monetary(currency_field='currency_id')

    balance = fields.Monetary()
    part_debit_amount = fields.Monetary()
    part_credit_amount = fields.Monetary()
    amount_check = fields.Float()

    currency_id = fields.Many2one('res.currency')
    amount_currency = fields.Monetary(currency_field='currency_id')

    invoice_date = fields.Date()
    invoice_date_due = fields.Date()

    @api.model
    def format_value(self, amount, currency=False, blank_if_zero=False, currency_char=True):
        currency_id = currency or self.env.company.currency_id
        if currency_id.is_zero(amount):
            if blank_if_zero:
                return ''
            # don't print -0.0 in reports
            amount = abs(amount)

        if self.env.context.get('no_format'):
            return amount

        if not currency_char:
            return formatLang(self.env, amount, currency_obj=False)

        return formatLang(self.env, amount, currency_obj=currency_id)

    @api.model
    def format_value_nch(self, amount, currency=False, blank_if_zero=False):
        currency_id = currency or self.env.company.currency_id
        if currency_id.is_zero(amount):
            if blank_if_zero:
                return ''
            # don't print -0.0 in reports
            amount = abs(amount)

        if self.env.context.get('no_format'):
            return amount

        return formatLang(self.env, amount, currency_obj=False)

    def _get_hierarchy_details(self, options):
        return [
            self._hierarchy_level('partner_id', foldable=True, namespan=len(self._get_column_details(options)) - 2),
            self._hierarchy_level('id'),
        ]

    def _format_id_line(self, res, value_dict, options):
        res['name'] = value_dict['move_name']
        res['caret_options'] = 'account.payment' if value_dict.get('payment_id') else 'account.move'
        for col in res['columns']:
            if col.get('no_format') == 0:
                col['name'] = ''
        res['columns'][-1]['name'] = ''

        # l = len(self._get_column_details(options)) - 1
        #
        # res['columns'][int(l - 2)] = {'name': '', 'no_format': False}
        # res['columns'][int(l - 3)] = {'name': '', 'no_format': False}
        # res['columns'][int(l - 4)] = {'name': '', 'no_format': False}
        # res['columns'][int(l - 5)] = {'name': '', 'no_format': False}
        # res['columns'][int(l - 6)] = {'name': '', 'no_format': False}
        # res['columns'][int(l - 7)] = {'name': '', 'no_format': False}

    def _format_total_line(self, res, value_dict, options):
        res['name'] = _('Total')
        res['colspan'] = len(self._get_column_details(options)) - 2
        res['columns'] = res['columns'][res['colspan'] - 1:]

    def _field_column(self, field_name, sortable=False, name=None, ellipsis=False, blank_if_zero=False,
                      currency_char=False):
        classes = ['text-nowrap']

        def getter(v):
            return self._fields[field_name].convert_to_cache(v.get(field_name, ''), self)

        if self._fields[field_name].type in ['float']:
            classes += ['number']

            def formatter(v):
                return v if v or not blank_if_zero else ''
        elif self._fields[field_name].type in ['monetary']:
            classes += ['number']

            def m_getter(v):
                return (v.get(field_name, ''), self.env['res.currency'].browse(
                    v.get(self._fields[field_name].currency_field, (False,))[0])
                        )

            getter = m_getter

            def formatter(v):
                return self.format_value(v[0], v[1], blank_if_zero=blank_if_zero, currency_char=currency_char)
        elif self._fields[field_name].type in ['char']:
            classes += ['text-center']

            def formatter(v):
                return v
        elif self._fields[field_name].type in ['date']:
            classes += ['date']

            def formatter(v):
                return format_date(self.env, v)
        elif self._fields[field_name].type in ['many2one']:
            classes += ['text-center']

            def r_getter(v):
                return v.get(field_name, False)

            getter = r_getter

            def formatter(v):
                return v[1] if v else ''

        IrModelFields = self.env['ir.model.fields']
        return self._custom_column(name=name or IrModelFields._get(self._name, field_name).field_description,
                                   getter=getter,
                                   formatter=formatter,
                                   classes=classes,
                                   sortable=sortable)

    @api.model
    def _get_sql(self):
        options = self.env.context['report_options']
        query = ("""
                SELECT
                    {move_line_fields},
                    account_move_line.amount_currency as amount_currency,
                    account_move_line.partner_id AS partner_id,
                    partner.name AS partner_name,
                    COALESCE(trust_property.value_text, 'normal') AS partner_trust,
                    COALESCE(account_move_line.currency_id, journal.currency_id) AS report_currency_id,
                    account_move_line.payment_id AS payment_id,
                    COALESCE(account_move_line.date_maturity, account_move_line.date) AS report_date,
                    account_move_line.expected_pay_date AS expected_pay_date,
                    move.move_type AS move_type,
                    move.name AS move_name,
                    move.ref AS move_ref,
                    move.invoice_date AS invoice_date,
                    move.invoice_date_due as invoice_date_due,
                    journal.code AS journal_code,
                    
                    COALESCE(SUM(part_debit.amount_currency), 0) AS amount_paid,
                    (account_move_line.amount_currency - COALESCE(SUM(part_debit.amount_currency), 0)) AS amount_residual,
                    
                    
                     CASE WHEN (account_move_line.amount_currency+COALESCE(SUM(part_debit.amount_currency), 0)) != 0
                    THEN (account_move_line.amount_currency - COALESCE(SUM(part_debit.amount_currency), 0))/COALESCE(curr_rate.rate, 1)
                    ELSE (ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), currency_table.precision))
                    END AS amount_total_hkd,   
                            
                                            
                    COALESCE(SUM(part_debit.amount), 0) AS part_debit_amount,
                    COALESCE(SUM(part_credit.amount), 0) AS part_credit_amount,
                    
                    ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), currency_table.precision) AS amount_check,
                    
                    COALESCE(so.name, move.ref) AS order_no,
                    curr_rate.rate AS currency_rate,
                    
                    account.code || ' ' || account.name AS account_name,
                    account.code AS account_code,""" + ','.join([("""
                    CASE WHEN period_table.period_index = {i}
                    THEN %(sign)s * ROUND((
                        account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0)
                    ) * currency_table.rate, currency_table.precision)
                    ELSE 0 END AS period{i}""").format(i=i) for i in range(6)]) + """
                FROM account_move_line
                JOIN account_move move ON account_move_line.move_id = move.id
                LEFT JOIN sale_order so ON move.x_studio_source_order = so.id
                JOIN account_journal journal ON journal.id = account_move_line.journal_id
                JOIN account_account account ON account.id = account_move_line.account_id
                LEFT JOIN res_partner partner ON partner.id = account_move_line.partner_id
                LEFT JOIN ir_property trust_property ON (
                    trust_property.res_id = 'res.partner,'|| account_move_line.partner_id
                    AND trust_property.name = 'trust'
                    AND trust_property.company_id = account_move_line.company_id
                )
                JOIN {currency_table} ON currency_table.company_id = account_move_line.company_id
                LEFT JOIN LATERAL (
                    SELECT cr_c1.currency_id, cr_c1.rate
                    FROM res_currency_rate cr_c1
                    WHERE cr_c1.currency_id = account_move_line.currency_id
                    AND cr_c1.name <= %(date)s
                    ORDER BY cr_c1.name DESC 
                    LIMIT 1
                ) curr_rate ON account_move_line.currency_id = curr_rate.currency_id
                LEFT JOIN LATERAL (
                    SELECT part.amount, part.debit_move_id, part.debit_amount_currency AS amount_currency
                    FROM account_partial_reconcile part
                    WHERE part.max_date <= %(date)s
                ) part_debit ON part_debit.debit_move_id = account_move_line.id
                LEFT JOIN LATERAL (
                    SELECT part.amount, part.credit_move_id
                    FROM account_partial_reconcile part
                    WHERE part.max_date <= %(date)s
                ) part_credit ON part_credit.credit_move_id = account_move_line.id
                JOIN {period_table} ON (
                    period_table.date_start IS NULL
                    OR COALESCE(account_move_line.date_maturity, account_move_line.date) <= DATE(period_table.date_start)
                )
                AND (
                    period_table.date_stop IS NULL
                    OR COALESCE(account_move_line.date_maturity, account_move_line.date) >= DATE(period_table.date_stop)
                )
                WHERE account.internal_type = %(account_type)s
                GROUP BY account_move_line.id, partner.id, trust_property.id, journal.id, move.id, account.id,
                         period_table.period_index, currency_table.rate, currency_table.precision, 
                         so.id,
                         curr_rate.rate
                HAVING ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), currency_table.precision) != 0
            """).format(
            move_line_fields=self._get_move_line_fields('account_move_line'),
            currency_table=self.env['res.currency']._get_query_currency_table(options),
            period_table=self._get_query_period_table(options),
        )
        params = {
            'account_type': options['filter_account_type'],
            'sign': 1 if options['filter_account_type'] == 'receivable' else -1,
            'date': options['date']['date_to'],
        }

        return self.env.cr.mogrify(query, params).decode(self.env.cr.connection.encoding)

    @api.model
    def _get_column_details(self, options):
        columns = [
            self._header_column(),
            # self._field_column('report_date'),
            self._field_column('invoice_date', name=_("Accounting date")),

            # self._field_column('balance', name=_("balance")),
            # self._field_column('part_debit_amount', name=_("part_debit_amount")),
            # self._field_column('part_credit_amount', name=_("part_credit_amount")),
            # self._field_column('amount_check', name=_("amount_check")),

            self._field_column('amount_residual', name=_("Balance in original currency Amount")),
            self._field_column('amount_currency', name=_("Original Invoice Amount")),
            self._field_column('currency_id', name=_("Currency")),
            self._field_column('currency_rate', name=_("Rate")),
            self._field_column('invoice_date_due', name=_("Expiration Since")),
            self._custom_column(  # Avoid doing twice the sub-select in the view
                name=_('Invoice with as of rate (HKD)'),
                classes=['number'],
                formatter=self.format_value,
                getter=(
                    lambda v: (v['amount_currency'] or 0) / (v['currency_rate'] or 1)),
                sortable=False,
            ),

            self._field_column('account_name', name=_("Account")),
            self._field_column('order_no', name=_("Order No.")),

            # self._field_column('expected_pay_date'),
            self._field_column('amount_total_hkd', name=_('Total (HKD)')),
            self._custom_column(  # Avoid doing twice the sub-select in the view
                name=_('Total'),
                classes=['number', 'd-none'],
                formatter=self.format_value,
                getter=(
                    lambda v: v['amount_total_hkd']),
                sortable=True,
            ),
            # self._custom_column(  # Avoid doing twice the sub-select in the view
            #     name=_('Sub Total'),
            #     classes=['number'],
            #     formatter=self.format_value_nch,
            #     getter=(
            #         lambda v: v['period0'] + v['period1'] + v['period2'] + v['period3'] + v['period4'] + v['period5']),
            #     sortable=False,
            # ),

            # self._field_column('period0', name=_("As of: %s", format_date(self.env, options['date']['date_to'])),
            #                    currency_char=False),
            # self._field_column('period1', sortable=True),
            # self._field_column('period2', sortable=True),
            # self._field_column('period3', sortable=True),
            # self._field_column('period4', sortable=True),
            # self._field_column('period5', sortable=True),

            # self._custom_column(  # Avoid doing twice the sub-select in the view
            #     name=_('Total'),
            #     classes=['number', 'd-none'],
            #     formatter=self.format_value,
            #     getter=(
            #         lambda v: v['period0'] + v['period1'] + v['period2'] + v['period3'] + v['period4'] + v['period5']),
            #     sortable=True,
            # ),
        ]

        return columns


class ReportAccountAgedPayableCustomize(models.Model):
    _inherit = "account.aged.payable"
    _auto = False

    @api.model
    def _get_sql(self):
        options = self.env.context['report_options']
        query = ("""
                        SELECT
                        {move_line_fields},
                        account_move_line.amount_currency as amount_currency,
                        account_move_line.partner_id AS partner_id,
                        partner.name AS partner_name,
                        COALESCE(trust_property.value_text, 'normal') AS partner_trust,
                        COALESCE(account_move_line.currency_id, journal.currency_id) AS report_currency_id,
                        account_move_line.payment_id AS payment_id,
                        COALESCE(account_move_line.date_maturity, account_move_line.date) AS report_date,
                        account_move_line.expected_pay_date AS expected_pay_date,
                        move.move_type AS move_type,
                        move.name AS move_name,
                        move.ref AS move_ref,
                        move.invoice_date AS invoice_date,
                        move.invoice_date_due as invoice_date_due,
                        journal.code AS journal_code,

                        COALESCE(SUM(part_credit.amount_currency), 0) AS amount_paid,
                        (account_move_line.amount_currency + COALESCE(SUM(part_credit.amount_currency), 0)) AS amount_residual,
                        ((account_move_line.amount_currency + COALESCE(SUM(part_credit.amount_currency), 0)) / COALESCE(curr_rate.rate, 1)) AS amount_total_hkd,

                        COALESCE(SUM(part_debit.amount), 0) AS part_debit_amount,
                        COALESCE(SUM(part_credit.amount), 0) AS part_credit_amount,
                        
                        ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), 0) AS amount_check,
                        
                        COALESCE(so.name, move.ref) AS order_no,
                        curr_rate.rate AS currency_rate,
                        
                        account.code || ' ' || account.name AS account_name,
                        account.code AS account_code,""" + ','.join([("""
                        CASE WHEN period_table.period_index = {i}
                        THEN %(sign)s * ROUND((
                            CASE WHEN curr_rate.rate > 0
                            THEN (account_move_line.amount_currency - COALESCE(SUM(part_debit.amount_currency), 0) + COALESCE(SUM(part_credit.amount_currency), 0)) / curr_rate.rate
                            ELSE (account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0))
                            END
                        ) * currency_table.rate, currency_table.precision)
                        ELSE 0 END AS period{i}""").format(i=i) for i in range(6)]) + """
                    FROM account_move_line
                    JOIN account_move move ON account_move_line.move_id = move.id
                    LEFT JOIN sale_order so ON move.x_studio_source_order = so.id
                    JOIN account_journal journal ON journal.id = account_move_line.journal_id
                    JOIN account_account account ON account.id = account_move_line.account_id
                    LEFT JOIN res_partner partner ON partner.id = account_move_line.partner_id
                    LEFT JOIN ir_property trust_property ON (
                        trust_property.res_id = 'res.partner,'|| account_move_line.partner_id
                        AND trust_property.name = 'trust'
                        AND trust_property.company_id = account_move_line.company_id
                    )
                    JOIN {currency_table} ON currency_table.company_id = account_move_line.company_id
                    LEFT JOIN LATERAL (
                        SELECT cr_c1.currency_id, cr_c1.rate
                        FROM res_currency_rate cr_c1
                        WHERE cr_c1.currency_id = account_move_line.currency_id
                        AND cr_c1.name <= %(date)s
                        ORDER BY cr_c1.name DESC 
                        LIMIT 1
                    ) curr_rate ON account_move_line.currency_id = curr_rate.currency_id
                    LEFT JOIN LATERAL (
                        SELECT part.amount, part.debit_move_id, part.debit_amount_currency AS amount_currency
                        FROM account_partial_reconcile part
                        WHERE part.max_date <= %(date)s
                    ) part_debit ON part_debit.debit_move_id = account_move_line.id
                    LEFT JOIN LATERAL (
                        SELECT part.amount, part.credit_move_id, part.debit_amount_currency AS amount_currency
                        FROM account_partial_reconcile part
                        WHERE part.max_date <= %(date)s
                    ) part_credit ON part_credit.credit_move_id = account_move_line.id
                    JOIN {period_table} ON (
                        period_table.date_start IS NULL
                        OR COALESCE(account_move_line.date_maturity, account_move_line.date) <= DATE(period_table.date_start)
                    )
                    AND (
                        period_table.date_stop IS NULL
                        OR COALESCE(account_move_line.date_maturity, account_move_line.date) >= DATE(period_table.date_stop)
                    )
                    WHERE account.internal_type = %(account_type)s
                    GROUP BY account_move_line.id, partner.id, trust_property.id, journal.id, move.id, account.id,
                             period_table.period_index, currency_table.rate, currency_table.precision, 
                             so.id,
                             curr_rate.rate
                    HAVING ROUND(account_move_line.amount_currency - COALESCE(SUM(part_debit.amount_currency), 0) + COALESCE(SUM(part_credit.amount_currency), 0), 0) != 0
                """).format(
            move_line_fields=self._get_move_line_fields('account_move_line'),
            currency_table=self.env['res.currency']._get_query_currency_table(options),
            period_table=self._get_query_period_table(options),
        )
        # HAVING ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), 0) != 0
        # HAVING ROUND(account_move_line.amount_currency + COALESCE(SUM(part_credit.amount_currency), 0)) != 0
        params = {
            'account_type': options['filter_account_type'],
            'sign': 1 if options['filter_account_type'] == 'receivable' else -1,
            'date': options['date']['date_to'],
        }
        return self.env.cr.mogrify(query, params).decode(self.env.cr.connection.encoding)
