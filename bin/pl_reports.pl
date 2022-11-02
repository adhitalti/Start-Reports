#!/usr/bin/perl

###############################################################################
#
# Script: pl_reports.pl
#
# Description: Create P&L reports for the Sales Team.
#
###############################################################################

require "/data/packages/common/lib/common_lib.pm";
use Getopt::Std;
use Tie::IxHash;
use JSON::Parse 'read_json';
use Storable qw(dclone);

our %opts;

###############################################################################
# Usage statement.

sub usage {
	my $message = $_[0];
	my $usage_statement;
	my $usage_statement_1 = 'usage: client_onboarding_reports.pl [-hq] [-d date]';
	my $usage_statement_2 = '
	General options:

	-d date or date_offset: Run report for this date or date_offset.
		Default is today\'s date.
	-h: Display full usage text.
	-q: Query verbose mode.  Output all queries executed and their results
		to the screen and to the log file.

	Notes:

	- date_offset is a date offset relative to today.
		- Format: n%[dHmMSY] where n is an integer and %[dHmMSY] is the date
			portion.  The date portion coincides to formats accepted by POSIX
			functions.  The following are accepted:
			%d: days
			%H: hours
			%m: months
			%M: minutes
			%S: seconds
			%Y: years
		- Examples:
			1%Y: process for today\'s date plus one year.
			-1%Y: process for today\'s date minus one year.
			-1%m: process for today\'s date minus one month.
			-2%d: process for today\'s date minus two days.';

	if ( defined($opts{h}) ) {
		$usage_statement = "\n\n\t$usage_statement_1\n$usage_statement_2";
	}
	else {
		$usage_statement = "\n\n\t$message\n\n\t$usage_statement_1\n\n\tuse -h to display full usage text.";
	}

	die "$usage_statement\n\n";
}

###############################################################################
# Process options.

getopts('d:hq', \%opts);

###############################################################################
# Display help text when -h is specified.

if ( defined($opts{h}) ) {
	usage();
}

###############################################################################
# Initialization.  Environment variables are stored in %env.

initialize(
	process_title => 'P&L Reports'
) or die "Initialization failed because of the following:\n\n$error_string";

###############################################################################
# Store the command line.

my @command_line = ("$0");

foreach my $key (sort(keys(%opts))) {
	if ( $opts{$key} eq "1" ) {
		push (@command_line, "-$key");
	}
	else {
		push (@command_line, "-$key $opts{$key}");
	}
}

push (@command_line, @ARGV);
$env{command_line} = join(" ", @command_line);

###############################################################################
# Set default values for the following options.

my $config_file = '/data/packages/client_onboarding/config/pl_reports.json';
my $db_file = '/data/packages/common/config/db_env_aliases.json';

$opts{q} = $opts{q} // "0"; # Do not output executed queries.

###############################################################################
# Read configuration files into memory.

my %config_objects;
my %config_files = (
	'db' => "$db_file",
	'pl_reports' => "$config_file"
);

foreach my $config_name ( keys(%config_files) ) {
	if ( ! ( $config_objects{$config_name} = read_json("$config_files{$config_name}") ) ) {
		die "Cannot read in config file $config_files{$config_name}.\n\t$!";
	}
}

###############################################################################
# Grab object properties to apply to all clients.

my %local_env = %{$config_objects{pl_reports}{_ENV}{$env{environment}}};

###############################################################################
# Override the following environment variables with configuration file values.

$env{support} = join(", ",
	(
		defined($local_env{support}{$env{logname}}{recipients}) ?
			@{$local_env{support}{$env{logname}}{recipients}} :
			( split(/ *, */, "$env{support}") )
	)
);

$env{success_messages} = $local_env{support}{$env{logname}}{success_messages} // "$env{success_messages}";
$env{warning_messages} = $local_env{support}{$env{logname}}{warning_messages} // "$env{warning_messages}";
$env{failure_messages} = $local_env{support}{$env{logname}}{failure_messages} // "$env{failure_messages}";

###############################################################################
# Print environment.

my @env_list = ();

foreach my $env_key (sort(keys(%env))) {
	push(@env_list, "$env_key = $env{$env_key}");
}
logprint("Environment: \n\t" . join("\n\t", @env_list) . "\n\n");

###############################################################################
# Validate existence of required configuration properties.

my @required_db_properties = ( 'host', 'port', 'user', 'password' );
my @db_aliases = ('clob_db');

foreach my $property (@required_db_properties) {
	foreach my $alias (@db_aliases) {
		if ( ! defined($config_objects{db}{$env{environment}}{$alias}{$property}) ) {
			die "Property $property for alias clob_db in environment $env{environment} in $db_file is required.";
		}
	}
}

my %db = %{$config_objects{db}{$env{environment}}};

###############################################################################
# Create connection object.

my $query;
my $recordset;

logprint("Creating master database connection object to $db{clob_db}{host}:$db{clob_db}{port}.");

mysql_connect(
	connection_name => "clob_db",
	host => "$db{clob_db}{host}",
	port => "$db{clob_db}{port}",
	database => 'pl',
	username => "$db{clob_db}{user}",
	password => "$db{clob_db}{password}",
	verbose => "$opts{q}"
) or die "Could not connect to $db{clob_db}{host}:$db{clob_db}{port}\n\t$error_string";

###############################################################################
# Validate date options.

my %date_option_names = (
	d => 'process_date_int'
);

my %dates;

foreach my $date_option ( keys(%date_option_names) ) {
	if ( defined($opts{$date_option}) ) {
		if ( $opts{$date_option} =~ /\%/ ) {
			$dates{$date_option_names{$date_option}} = return_datetime(format => '%Y%m%d', offset => "$opts{$date_option}")
				or die "Option -$date_option: Invalid date_offset $opts{$date_option} entered.";
		}
		else {
			$dates{$date_option_names{$date_option}} = return_datetime(format => '%Y%m%d', date_time => "$opts{$date_option}")
				or die "Option -$date_option: Invalid date $opts{$date_option} entered.";
		}
	}
}

###############################################################################
# Set default dates.

$dates{'process_date_int'} = $dates{'process_date_int'} // return_datetime(format => '%Y%m%d');
$dates{'today_date_int'} = return_datetime(format => '%Y%m%d');
$dates{'process_date_us'} = return_datetime(format => '%m/%d/%Y', date_time => "$dates{'process_date_int'}");
$dates{'process_date_mysql'} = return_datetime(format => '%Y-%m-%d', date_time => "$dates{'process_date_int'}");

###############################################################################
# Validate dates.

if ( $dates{'process_date_int'} > $dates{'today_date_int'} ) {
	die "Process date $dates{'process_date_us'}" . ( defined($opts{d}) ? ' (option -d) ' : ' ' ) . "cannot be in the future.\n";
}

logprint("Creating reports for $dates{'process_date_us'}.\n\n");

###############################################################################
# Report of active clients who have yet to upload files to the SFTP server.
# For each client_code returned, determine which files were found and which are
# still missing and append these fields to the output.

use Tie::IxHash;
my ($month, $year, $month_name) = split(/,/, return_datetime(format => '%m,%Y,%B', date_time => "$dates{'process_date_int'}"));
my $filename_date = "${year}${month}-${month_name}";

my %report_files = (
	'Totals per Broker' => "$env{output_dirname}/totals_per_broker_${filename_date}.csv",
	'Client Orders' => "$env{output_dirname}/client_orders_${filename_date}.csv"
);

my $number_of_records;

###############################################################################
# Create reports.

my %reports;
tie %reports, 'Tie::IxHash';

%reports = (
	'Totals per Broker'
		=> "SELECT
				client AS 'Client',
				broker AS 'Broker',
				sum(total_shares) AS 'Total Shares',
				sum(notional_value) AS 'Notional Value',
				sum(total_commission) AS 'Total Commission'
			FROM
				pl.daily_totals
			WHERE
				MONTH(trade_date) = $month
				AND YEAR(trade_date) = $year
			GROUP BY
				client,
				broker
			HAVING
				sum(total_shares) > 0
			ORDER BY
				client,
				broker
		",
	'Client Orders'
		=> "SELECT
				*
			FROM
				pl.orders
			WHERE
				MONTH(trade_date) = $month
				AND YEAR(trade_date) = $year
			ORDER BY
				client,
				broker,
				trade_date
		"
);

while ( my ($title, $query) = each(%reports) ) {

	open (REPORT, ">$report_files{$title}") or die "File $report_files{$title} could not be opened for writing.\n\t$!\n";

	logprint("Running report: $title for $month_name $year.");
	print REPORT "$title ($month_name $year)\n\n";

	$recordset = mysql_execute_query (
		connection_name => "clob_db",
		query => "$query",
		preserve_field_order => "1",
		verbose => "$opts{q}"
	) or die "$error_string\n";

	if ( ! ( @formatted_records = format_recordset (recordset => $recordset, type => 'csv') ) ) {
		print REPORT "No records returned\n";
		logprint("No records were returned.\n\n");
	}
	else {
		$number_of_records = scalar(@formatted_records);
		logprint("Report generated with $number_of_records records.\n\n");
		print REPORT join("\n", @formatted_records) . "\n";
	}
	close (REPORT);
}

logprint("Reports complete.\n\n");

###############################################################################
# Close the file and email the contents.

my $data = "<html><body>
Hello-<p>
Please find attached the following P&L reports for $month_name $year:<p><ul><li>" . join("</li><li>", keys(%reports)) . "</li></ul><p>
Thank you.<p>
</body></html>";

my $distribution_list = join(", ",
	(
		defined($local_env{reports_distribution_list}) ?
			@{$local_env{reports_distribution_list}} :
			( split(/ *, */, "$env{logname}\@abelnoser.com") )
	)
);

my @attachments = values(%report_files);

logprint("Sending report to " . join(", ", split(/\s+,\s+/, $distribution_list)));
notify (
	Data => "$data",
	Type => 'text/html',
	To => "$distribution_list",
	Subject => "P&L Reports for $month_name $year",
	Attachments => \@attachments,
	include_header => "0",
	include_footer => "0"
);
logprint("Complete\n\n");

###############################################################################
# End program;

exit_program();