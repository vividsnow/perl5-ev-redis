use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Scalar::Util qw(weaken);
use EV;
use EV::Redis;

$SIG{PIPE} = 'IGNORE';
$SIG{ALRM} = sub { BAIL_OUT('Lifecycle test exceeded 180 seconds') };
alarm 180;

my $seed = $ENV{EV_REDIS_SOAK_SEED} // 27491;
my $iterations = $ENV{EV_REDIS_SOAK_ITERATIONS} // 500;
die "Invalid seed\n" unless $seed =~ /\A\d+\z/;
die "Invalid iteration count\n" unless $iterations =~ /\A\d+\z/ && $iterations > 0 && $iterations <= 10000;
srand $seed;
diag "Lifecycle seed=$seed iterations=$iterations perl=$^V";

my $server = eval { Test::RedisServer->new };
BAIL_OUT("redis-server is required: $@") unless $server;
my %connect_info = $server->connect_info;
my (@errors, $running);
my $helper = EV::Redis->new(path => $connect_info{sock}, on_error => sub { push @errors, $_[0] });
my $r;

sub await {
    my ($done, $label) = @_;
    return if $done->();
    my $expired;
    EV::now_update;
    my $guard = EV::timer 10, 0, sub { $expired = 1; EV::break };
    while (!$done->() && !$expired) {
        $running = 1;
        EV::run;
        $running = 0;
    }
    BAIL_OUT("seed=$seed: timed out waiting for $label") if $expired;
}

sub queue {
    my ($client, @command) = @_;
    my $row = { calls => 0, reply => [], command => $command[0] };
    $client->command(@command, sub {
        $row->{calls}++;
        fail("$row->{command} callback ran more than once") if $row->{calls} > 1;
        $row->{reply} = [$_[0], $_[1]];
        EV::break if $running;
    });
    return $row;
}

sub request {
    my ($client, @command) = @_;
    my $row = queue($client, @command);
    await(sub { $row->{calls} }, "@command");
    is $row->{calls}, 1, "$command[0] callback ran once";
    is $row->{reply}[1], undef, "$command[0] succeeded";
    return $row->{reply}[0];
}

sub check_replies {
    my ($rows, $expected) = @_;
    is_deeply [map { [$_->{calls}, @{$_->{reply}}] } @$rows],
        [map { [1, @$_] } @$expected], 'each command received its own reply exactly once';
}

my $client_count = scalar(split /\n/, request($helper, 'CLIENT', 'LIST'));
my @scenarios = qw(pipeline transaction pubsub reconnect timeout destroy cancel);
my @limits = (0, 1, 3, 8);
for my $step (1..$iterations) {
    # Exercise every scenario even in the short Valgrind run, then vary their
    # order and parameters while reusing the same client across transitions.
    my $scenario = $step <= @scenarios ? $scenarios[$step - 1] : $scenarios[int rand @scenarios];
    my $limit = $limits[int rand @limits];
    my $protocol = 2 + int rand 2;
    subtest "$step: $scenario RESP$protocol max_pending=$limit" => sub {
        @errors = ();
        $r ||= EV::Redis->new(on_error => sub { push @errors, $_[0] });
        $r->connect_unix($connect_info{sock}) unless $r->is_connected;
        $r->max_pending($limit);
        my $hello = request($r, 'HELLO', $protocol);
        ok ref($hello) eq 'ARRAY', 'negotiated the protocol';
        my $key = "ci:soak:$seed:$step";
        my $value = "$key\0" . ('x' x (int(rand(4)) ? 32 : 16384));
        my (@rows, @expected);

        if ($scenario eq 'pipeline') {
            my ($stored, $counter);
            for my $i (1..5 + int rand 16) {
                my $operation = int rand 4;
                if ($operation == 0) {
                    push @rows, queue($r, 'ECHO', "$value:$i");
                    push @expected, ["$value:$i", undef];
                } elsif ($operation == 1) {
                    $stored = "$value:$i";
                    push @rows, queue($r, 'SET', $key, $stored);
                    push @expected, ['OK', undef];
                } elsif ($operation == 2) {
                    push @rows, queue($r, 'GET', $key);
                    push @expected, [$stored, undef];
                } else {
                    push @rows, queue($r, 'INCR', "$key:counter");
                    push @expected, [++$counter, undef];
                }
            }
        } elsif ($scenario eq 'transaction') {
            my $abort = int rand 2;
            push @rows, queue($r, 'MULTI'), queue($r, 'SET', $key, $value), queue($r, 'GET', $key);
            push @expected, ['OK', undef], ['QUEUED', undef], ['QUEUED', undef];
            push @rows, queue($r, $abort ? 'DISCARD' : 'EXEC'), queue($r, 'ECHO', $key);
            push @expected, [$abort ? 'OK' : ['OK', $value], undef], [$key, undef];
        } elsif ($scenario eq 'pubsub') {
            my @channels = map { "$key:$_" } 1..1 + int rand 3;
            my @events;
            $r->subscribe(@channels, sub {
                push @events, [$_[0], $_[1]];
                EV::break if $running;
            });
            await(sub { @events >= @channels }, 'subscription acknowledgements');
            my $count = 0;
            push @expected, [ ['subscribe', $_, ++$count], undef ] for @channels;
            for my $channel (@channels) {
                is request($helper, 'PUBLISH', $channel, $value), 1, 'one subscriber received the message';
                push @expected, [ ['message', $channel, $value], undef ];
            }
            await(sub { @events >= 2 * @channels }, 'published messages');
            $r->unsubscribe(@channels);
            push @expected, [ ['unsubscribe', $_, --$count], undef ] for @channels;
            await(sub { @events >= 3 * @channels }, 'unsubscribe acknowledgements');
            is_deeply \@events, \@expected, 'subscription delivered each acknowledgement and message once';
            is request($r, 'PING'), 'PONG', 'ordinary commands work after unsubscribing';
        } elsif ($scenario eq 'reconnect') {
            my $id = request($r, 'CLIENT', 'ID');
            $r->max_pending(1);
            $r->reconnect(1, 10, 20);
            $r->resume_waiting_on_reconnect(1);
            push @rows, queue($r, 'BLPOP', "$key:absent", 0), queue($r, 'ECHO', $value);
            is request($helper, 'CLIENT', 'KILL', 'ID', $id), 1, 'closed the old connection';
            await(sub { $rows[0]{calls} && $rows[1]{calls} }, 'reconnected replies');
            is $rows[0]{calls}, 1, 'lost command callback ran once';
            is $rows[0]{reply}[0], undef, 'lost command has no reply';
            ok defined $rows[0]{reply}[1], 'lost command reports the connection error';
            check_replies([$rows[1]], [[$value, undef]]);
            ok $r->is_connected, 'client reconnected';
            $r->reconnect(0);
            $r->resume_waiting_on_reconnect(0);
        } elsif ($scenario eq 'timeout') {
            $r->max_pending(1);
            $r->command_timeout(50);
            push @rows, queue($r, 'BLPOP', "$key:absent", 0), queue($r, 'ECHO', $value);
            push @expected, [undef, 'Timeout'], [undef, 'Timeout'];
        } elsif ($scenario eq 'destroy') {
            my $weak = $r;
            weaken($weak);
            $r->max_pending(1);
            push @rows, queue($r, 'BLPOP', "$key:absent", 0);
            push @expected, [undef, 'disconnected'];
            for (1..1 + int rand 8) {
                push @rows, queue($r, 'ECHO', $value);
                push @expected, [undef, 'disconnected'];
            }
            undef $r;
            ok !defined $weak, 'client was destroyed with both queues populated';
        } elsif ($scenario eq 'cancel') {
            my $all = int rand 2;
            $r->max_pending(1);
            for my $i (1..2 + int rand 8) {
                push @rows, queue($r, 'ECHO', "$value:$i");
                push @expected, $all || $i > 1 ? [undef, 'skipped'] : ["$value:$i", undef];
            }
            $all ? $r->skip_pending : $r->skip_waiting;
            push @rows, queue($r, 'ECHO', $key);
            push @expected, [$key, undef];
        }

        if (@rows && $scenario ne 'reconnect') {
            await(sub { !grep { !$_->{calls} } @rows }, "$scenario replies");
            check_replies(\@rows, \@expected);
        }
        if ($r) {
            is $r->pending_count, 0, 'pending queue is empty';
            is $r->waiting_count, 0, 'waiting queue is empty';
            $r->command_timeout(0);
        }
        if ($scenario eq 'reconnect') {
            is scalar(@errors), 1, 'connection loss was reported once';
        } else {
            is_deeply \@errors, $scenario eq 'timeout' ? ['Timeout'] : [], 'only expected connection errors occurred';
        }
    };
}

if ($r) {
    $r->disconnect;
    await(sub { !$r->is_connected }, 'final disconnect');
    undef $r;
}
# Only the helper and the server fixture's control connections should remain.
my $clients;
for (1..10) {
    $clients = request($helper, 'CLIENT', 'LIST');
    last if scalar(split /\n/, $clients) == $client_count;
}
is scalar(split /\n/, $clients), $client_count, 'no client connections leaked';
$helper->disconnect;
undef $helper;
alarm 0;
done_testing;
