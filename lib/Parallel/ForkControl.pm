#
# This is a nonsucking forking module.
# Coded by:
# 	Brad Lhotsky <brad@divisionbyzero.net>
# Contributions by:
#	Mark Thomas <mark@ackers.net>
#
package Parallel::ForkControl;
use strict;
use POSIX qw/:signal_h :errno_h :sys_wait_h/;

our $AUTOLOAD;
our $VERSION = 0.02;

use constant TRUE => 1;
use constant FALSE => 0;

use constant DEFAULT => 0;
use constant PERMISSION => 1;
use constant PERMIT_ALL => 'init/set/get/copy';

##################
# Debug constants
use constant DB_OFF	=> 0;
use constant DB_INFO	=> 1;
use constant DB_LOW	=> 2;
use constant DB_MED	=> 3;
use constant DB_HIGH	=> 4;

{
 # private data members
	my %_attributes = (
		# Name			     # defaults			# permissions
		'_name'			=> [ 'Unnamed Child',		PERMIT_ALL	],
		'_processtimeout'	=> [ 120,			PERMIT_ALL	],
		'_maxkids'		=> [ 5,				PERMIT_ALL	],
		'_minkids'		=> [ 1,				PERMIT_ALL	],
		'_maxload'		=> [ 4.50,			PERMIT_ALL	],
		'_maxmem'		=> [ 10.0,			PERMIT_ALL	],	# non functional
		'_maxcpu'		=> [ 25.0,			PERMIT_ALL	],
		'_method'		=> [ 'cycle',			PERMIT_ALL	],
		'_watchcount'		=> [ TRUE,			PERMIT_ALL	],
		'_watchload'		=> [ FALSE,			PERMIT_ALL	],
		'_watchmem'		=> [ FALSE,			PERMIT_ALL	],	# non functional
		'_watchcpu'		=> [ FALSE,			PERMIT_ALL	],	# non functional
		'_parentpid'		=> [ $$,			'get/set'	],
		'_code'			=> [ undef,			'init/get/set'	],
		'_debug'		=> [ DB_OFF,			'init/get/set'	],
		'_check_at'		=> [ 2,			PERMIT_ALL 	],
		'_checked'		=> [ 0,				'get/init'	]
	);
	
	my %_KIDS=();
	my $_KIDS=0;
 # private member accessors
	sub _attributes {
		# return an array of our attributes
		return keys %_attributes;
	}
	sub _default {
		# return the default for a set attribute
		my ($self,$attr) = @_;
		$attr =~ tr/[A-Z]/[a-z]/;
		$attr =~ s/^\s*_?/_/;
		return unless exists $_attributes{$attr};
		return $_attributes{$attr}->[DEFAULT];
	}
	sub _can {
		# return TRUE if we can $perm the $attr
		my ($self,$perm,$attr) = @_;	
		$attr =~ tr/[A-Z]/[a-z]/;
		$attr =~ s/^\s*_?/_/;
		return unless exists $_attributes{$attr};
		$perm =~ tr/[A-Z]/[a-z/;
		return TRUE if $_attributes{$attr}->[PERMISSION] =~ /$perm/;
		return FALSE;
	}
	sub _kidstarted {
		# keep records of our children
		my ($self,$kid) = @_;
		$self->_dbmsg(DB_LOW,"CHILD: $kid STARTING");
		#
		# use time() here to implement the process time out
		$_KIDS{$kid} = time;
		return ++$_KIDS;
	}
	sub _kidstopped {
		# keep track
		my ($self,$kid) = @_;
		$self->_dbmsg(DB_HIGH, "KIDSTOPPED: $kid");
		$self->_dbmsg(DB_HIGH, "KIDS: $_KIDS, (" . join(',',keys %_KIDS) . ")");
		return unless exists $_KIDS{$kid};
		$self->_dbmsg(DB_LOW,"CHILD: $kid ENDING");
		delete $_KIDS{$kid};
		return --$_KIDS;
	}
	sub kids {
		return wantarray() ? keys %_KIDS : $_KIDS;
	}
	sub kid_time {
		my ($self,$kid) = @_;
		return time unless exists $_KIDS{$kid};
		return $_KIDS{$kid};
	}
	sub _pid {
		return $$;
	}
}

# Class Methods
sub DESTROY {
	# load this into the symbol table immediately!
	my $self = shift;
	$self->cleanup();
}

sub new {
	# Constructor
	# Builds our initial Fork Object;
	my ($proto,@args) = @_;
	my $proto_is_obj = ref $proto;
	my $class = $proto_is_obj || $proto;
	my $self = bless {}, $class;
	# take care of capitalization:
	my %args=();
	while(@args) { 
		my $k = shift @args;
		my $v = shift @args;
		($k) = ($k =~ /^\s*_?(.*)$/);
		$args{lc($k)}=$v;
	}
	# now take care of our initialization
	foreach my $attr ($self->_attributes()) {
		my ($arg) = ($attr =~ /^_?(.*)/);
		# first see its in our argument list
		if(exists $args{$arg} && $self->_can('init',$attr)) {
			$self->{$attr} = $args{$arg};
		}
		# if not, check to see if we're copying an
		# object. Also, make sure we can copy it!
		elsif($proto_is_obj && $self->_can('copy', $attr)) {
			$self->{$attr} = $proto->{$attr};
		}
		# or, just use the default!
		else {
			$self->{$attr} = $self->_default($attr);
		}
	}
	# set the parent pid
	$self->set_parentpid($$);
	$self->_dbmsg(DB_HIGH,'FORK OBJECT CREATED');
	return $self;
}


sub _overLoad {
	# this is a cheap linux only hack
	# I will be replacing this as soon as I have time
	my $CMDTOCHECK = '/usr/bin/uptime';
	my ($self) = shift;
	return FALSE unless $self->get_watchload();
	open(LOAD, "$CMDTOCHECK |") or return FALSE;
	local $/ = undef;
	chomp(local $_ = <LOAD>);
	close LOAD;
	if(/load average\:\s+(\d+\.\d+)/m) {
		my $current = $1;
		my $MAXLOAD = $self->get_maxload();
		if ($current >= $MAXLOAD) {
			$self->_dbmsg(DB_LOW,"OVERLOAD: Current: $current, Max: $MAXLOAD, RETURNING TRUE");
			return TRUE;
		}
		$self->_dbmsg(DB_LOW,"OVERLOAD: Current: $current, Max: $MAXLOAD, RETURNING FALSE");
		return FALSE;
	}
	$self->_dbmsg(DB_LOW,'OVERLOAD: ERROR READING LOAD AVERAGE, RETURNING FALSE');
	return FALSE;
}

sub _tooManyKids {
	# determine if there are too many forks
	my ($self) = @_;
	my $kids = $self->kids;
	my $MAXKIDS = $self->get_maxkids();
	my $MINKIDS = $self->get_minkids();
	unless(	$self->get_watchload() ||
		$self->get_watchmem() ||
		$self->get_watchcpu()
	) {
		# not watching the load, stick to the
		# maxforks attribute
		$self->_dbmsg(DB_LOW,"TOOMANYKIDS - NOT CHECKING LOAD/MEM/CPU - Kids: $kids MAX: $MAXKIDS");
		if($kids >= $self->get_maxkids()) {
			$self->_dbmsg(DB_MED,'TOOMANYKIDS - RETURN TRUE');
			return TRUE;
		} else {
			$self->_dbmsg(DB_MED,'TOOMANYKIDS - RETURN FALSE');
			return FALSE;
		}
	}
	if($self->get_watchload) {
		$self->_dbmsg(DB_MED,'TOOMANYKIDS - LOAD CHECKING');
		if($self->get_watchcount) {
			if(!$self->_overLoad && ($kids < $MAXKIDS)) {
				$self->_dbmsg(DB_LOW,"TOOMANYKIDS - MAX: $MAXKIDS, Kids: $kids, Return: FALSE");
				return FALSE;
			}
			$self->_dbmsg(DB_LOW,"TOOMANYKIDS - MAX: $MAXKIDS, Kids: $kids, Return: TRUE");
			return TRUE;
		}
		else {
			$self->_dbmsg(DB_MED,'TOOMANYKIDS - CHECKING LOAD, NOT CHECKING COUNT');
			if(!$self->_overLoad) {
				$self->_dbmsg(DB_LOW,"TOOMANYKIDS - Kids: $kids, UNCHECKED RETURNING FALSE");
				return FALSE;
			}
			if($self->kids < $self->get_minkids) {
				$self->_dbmsg(DB_LOW, "TOOMANYKIDS - OVERLOAD BUT REACHING MINIMUM KIDS!");
				return FALSE;
			}
			$self->_dbmsg(DB_LOW,"TOOMANYKIDS - Kids: $kids, UNCHECKED RETURNING TRUE");
			return TRUE;
		}
	} # end of watchload

	# if we get to this point something is wrong, return true
	return TRUE;
}

sub _check {
	#
	# this function is here to make sure we don't
	# freeze up eventually.  It should be all good.
	my $self = shift;
	$self->{_checked}++;
	return if $self->get_check_at > $self->get_checked;
	foreach my $pid ( $self->kids ) {
		my $alive = kill 0, $pid;
		if($alive) {
			my $start = $self->kid_time($pid);
			if(time - $start > $self->get_processtimeout()) {
				kill 9, $pid;
			}
		}
	
		$self->_dbmsg(DB_INFO, "Child ($pid) evaded the reaper. Caught by _check()\n");
		$self->_kidstopped($pid);
	}
	$self->{_checked} = 0;
}

sub run {
	# self and args go in, run the code ref or die if
	# the code ref isn't set
	my ($self,@args) = @_;
	my $ref = ref $self->get_code;
	die "CANNOT RUN A $ref IN RUN()\n" unless $ref eq 'CODE';

	# return if our parent has died
	unless($self->_parentAlive()) {
		$self->_dbmsg(DB_MED, 'PARENT IS NOT ALIVE: ' . $self->get_parentpid);
		return;
	}

	# We might call _check();
	$self->_check();

	# wait for childern to die if we have too many
	if($self->get_method =~ /block/) {
		$self->cleanup() if $self->_tooManyKids;
	}
	elsif($self->_tooManyKids) {
		$self->_kidstopped(wait);
	}
	else {
		#
		# I have to throttle process creation for some reason
		# with this in here, we shouldn't be able to produce more
		# than 8 processes per second.  Its reasonable, but there
		# has to be a better way to deal with this.
		select undef, undef, undef, 0.1;
	}

	# Protect us from zombies
	$SIG{'CHLD'} =  sub { $self->_REAPER };

	# fork();
	my $pid = fork();
	# check for errors
	die "*\n* FORK ERROR !!\n*\n" unless defined $pid;

	# if we're the parent return
	if($pid > 0) {
		select undef, undef, undef, 0.01;
		return $self->_kidstarted($pid);
	}

	# we're the child, run and exit
	local $0 = '[ ' . $self->get_name . ' ]';
	$self->_dbmsg(DB_HIGH,'Running Fork Code');
	eval {
		#local $SIG{ALRM} = sub { die "timeout"; };
		#alarm $self->get_processtimeout if $self->get_processtimeout;
		$self->get_code()->(@args);
	};
	alarm 0;
	$self->_dbmsg(DB_LOW, "Child $$ timed out!") if $@ =~ /timeout/;
	my $CODE = $@ ? 1 : 0;
	exit $CODE;
}

sub cleanup {
	# We'll just rely on our SIG{'CHLD'} handler to actually
	# disperse of the children, so all we have to do is wait
	# here.
	my $self = shift;
	# using select here because it doesn't interfere
	# with any signals in the program
	while( $self->kids ) {
		$self->_check;
		select undef, undef, undef, 1;
	}
	return TRUE;
}

sub _REAPER {
	# our SIGCHLD Handler
	# Code from the Perl Cookbook page 592
	my $self = shift;
	my $pid = waitpid(-1, &WNOHANG);
	if($pid > 0) {
		# a pid did something,
		$self->_dbmsg(DB_HIGH,"Reaper found a child ($pid)!!!!!");
		if(!WIFEXITED($?)) {
			$self->_dbmsg(DB_INFO, "Child ($pid) exitted abnormally");
		}
		$self->_kidstopped($pid);
	}
	$SIG{'CHLD'} =  sub {$self->_REAPER};
}

sub _parentAlive {
	# check to see if the parent is still alive
	my $self = shift;
	return kill 0, $self->get_parentpid();
}

sub AUTOLOAD {
	# AUTOLOAD our get/set methods
	no strict 'refs';
	return if $AUTOLOAD =~ /DESTROY/;
	my ($self,$arg) = @_;

	# get routines
	if($AUTOLOAD =~ /get(_.*)/ && $self->_can('get', $1)) {
		my $attr = lc($1);
		*{$AUTOLOAD} = sub { return $_[0]->{$attr}; };
		return $self->{$attr};
	}

	# set routines
	if($AUTOLOAD =~ /set(_.*)/ && $self->_can('set', $1)){
		my $attr = lc($1);
		*{$AUTOLOAD} = sub {
					my ($self,$val) = @_;
					$self->{$attr} = $val;
					return $self->{$attr};
				};
		$self->{$attr} = $arg;
		return $self->{$attr};
	}

	warn "AUTOLOAD Could not find method $AUTOLOAD\n";
	return;
}

# DEBUG AND TESTING SUBS
sub print_me {
	my $self = shift;
	my $class = ref $self;
	print "$class Object:\n";
	foreach my $attr ($self->_attributes) {
		my ($pa) = ($attr =~ /^_(.*)/);
		$pa = "\L\u$pa";
		my $val = ref $self->{$attr} || $self->{$attr};
		print "\t$pa: $val\n";
	}
	print "\n";
}

sub _dbmsg {
	# print debugging messages:
	my ($self,$pri,@MSGS) = @_;
	return unless $self->get_debug() >= $pri;
	foreach my $msg (@MSGS) {
		$msg =~ s/[\cM\r\n]+//g;
		my $date = scalar localtime;
		print STDERR "$date - $msg\n";
	}
	return TRUE;
}

 return 1;
1
__END__

=head1 NAME

Parallel::ForkControl - Finer grained control of processes on a Unix System

=head1 SYNOPSIS

  use Parallel::ForkControl;
  my $forker = new Parallel::ForkControl(
				WatchCount		=> 1,
				MaxKids			=> 50,
				MinKids			=> 5,
				WatchLoad		=> 1,
				MaxLoad			=> 8.00
				Name			=> 'My Forker',
				Code			=> \&mysub
	);
  my @hosts = qw/host1 host2 host3 host5 host5/;
  foreach my $host (@hosts) {
	$forker->run($host);
  }

  $forker->cleanup();  # wait for all children to finish;
  .....

=head1 DESCRIPTION

Parallel::ForkControl introduces a new and simple way to deal with fork()ing.
The 'Code' parameter will be run everytime the run() method is called on the
fork object.  Any parameters passed to the run() method will be passed to the
subroutine ref defined as the 'Code' arg.  This allows a developer to spend
less time worrying about the underlying fork() system, and just write code.

=head1 METHODS

=over 4

=item B<new([ Option =E<gt> Value ... ])>

Constructor.  Creates a Parallel::ForkControl object for using.  Ideally,
all options should be set here and not changed, though the accessors and
mutators allow such behavior, even while the B<run()> method is being executed.

=over 4

=item Options

=over 4

=item Name

Process Name that will show up in a 'ps', mostly cosmetic, but serves as an
easy way to distinguish children and parent in a ps.

=item ProcessTimeOut

The max time any given process is allowed to run before its interrupted.
B<Default :>120 seconds

=item WatchCount

Enforce count (MaxKids) restraints on new processes.
B<Default :> 1

=item WatchLoad

Enforce load based (MaxLoad) restraints on process creation. NOTE: This MUST be
a true value to enable throttling based on Load Averages.
B<Default :> 0

=item WatchMem ***

(unimplemented)

=item WatchCPU ***

(unimplemented)

=item Method

May be 'block' or 'cycle'.  Block will fork off MaxKids and wait for all of them
to die, then fork off MaxKids more processes.  Cycle will continually replace
processes as the restraints allow.  Cycle is almost ALWAYS the preferred method.
B<Default :>Cycle
B

=item MaxKids

The maximum number of children that may be running at any given time.
B<Default :> 5

=item MinKids

The minimum number of kids to keep running regardless of load/memory/CPU
throttling.
B<Default :> 1

=item MaxLoad

The maximum one minute average load.  Make sure to set WatchLoad.
B<Default :> 4.50 (off by default)

=item MaxMem  *** 

(unimplemented)

=item MaxCPU ***

(unimplemented)

=item Code

This should be a subroutine reference.  If you intend on passing arguments to this
subroutine arguments it is imperative that you B<NOT> include () in the reference.
All code inside the subroutine will be run in the child process.  The module provides
all the necessary checks and safety nets, so your subroutine may just "return".  It is
not necessary, nor is it good practice to have exit()s in this subroutine as eventually,
return codes will be stored and made available to the parent process after completion.
Examples:

	my $code = sub {
			# do something useful
			my $t = shift;
			return $t;
	};

	my $forker = new Parallel::ForkControl(
				Name => 'me',
				MaxKids => 10,
				Code => $code
				# or
				#Code => \&mysub
	)

	sub mysub {
		my $t = shift;
		return $t;
	}

=item Check_At

This determines between how many child processes the module does some checking
to verify the validity of its internal process table.  It shouldn't be necessary 
to modify this value, but given it is a little low, someone only utilizing this
module for a larger number of data sets might want to check things at larger
intervals.
B<Default :> 2

=item Debug

A number 0-4. The higher the number, the more debugging information you'll see.
0 means nothing.
B<Default :> 0

=back

=back

=item B<run([ @ARGS ])>

This method calls the subroutine passed as the I<Code> option.  This method
handles process throttling, creation, monitoring, and reaping.  The subroutine
in the I<Code> option run in the child process and all control is returned to the
parent object as soon as the child is successfully created. B<run()> will block
until it is allowed to create a process or process creation fails completely.
B<run()> returns the number of kids on success, or undef on failure.  B<NOTE:> This
is not the return code of your subroutine.  I will eventually provide mapping
to argument sets passed to run() with success/failure options and (idea) a
"Report" option to enable some form of reporting based on that API.

=item B<cleanup()>

This method blocks until all children have finished processing.

=back

=head1 EXPORT

None by default.

=head1 KNOWN ISSUES

=item 01/08/2004 - brad@divisionbyzero.net

=over 4

For some reason, I'm having to throttle process creation, as a slew of  processes
starting and ending at the same time seems to be causing problems on my machine.
I've adjust the Check_At down to 2 which seems to catch any processes whose SIG{CHLD}
gets lost in the mess of spawning.  I'm looking into a more permanent, professional
solution.

=back

=head1 SEE ALSO

perldoc -f fork, search CPAN for Parallel::ForkManager

=head1 AUTHOR

Brad Lhotsky E<lt>brad@divisionbyzero.netE<gt>

=head1 CONTRIBUTIONS BY

Mark Thomas E<lt>mark@ackers.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2003 by Brad Lhotsky

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
