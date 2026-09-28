use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use IO::Socket::UNIX;
use File::Temp qw(tempdir);

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my %connect_info = $redis_server->connect_info;

use EV;
use EV::Redis;

$SIG{PIPE} = 'IGNORE';

# a listening socket that answers only when told to
my $dir = tempdir(CLEANUP => 1);
my $hung_path = "$dir/hung.sock";
my $hung = IO::Socket::UNIX->new(Local => $hung_path, Listen => 5) or die $!;

sub run_until {
    my ($done, $secs) = @_;
    my $expired;
    EV::now_update;
    my $g = EV::timer $secs, 0, sub { $expired = 1; EV::break };
    my $c = EV::prepare sub { EV::break if $done->() };
    EV::run until $done->() || $expired;
}

# replies owed by a connection disconnect() replaced hold no max_pending slot
{
    my @log;
    my $r = EV::Redis->new(path => $hung_path, max_pending => 1,
        on_error => sub { push @log, "error: $_[0]" });
    $r->get('never', sub { push @log, 'old: ' . ($_[1] // $_[0] // 'nil') });
    run_until(sub { 0 }, 0.2);

    is $r->pending_count, 1, 'one reply outstanding on the hung connection';
    $r->disconnect;
    $r->connect_unix($connect_info{sock});
    is $r->pending_count, 1, 'pending_count still counts the owed reply';
    $r->ping(sub { push @log, 'new: ' . ($_[1] // $_[0]) });
    is $r->waiting_count, 0, 'the new connection sends at once';
    run_until(sub { grep { /^new/ } @log }, 5);
    is_deeply \@log, ['new: PONG'], 'a command on the new connection is answered';

    # the hung server answers at last
    my $peer = $hung->accept;
    sysread $peer, my $req, 4096;
    syswrite $peer, "\$-1\r\n";
    run_until(sub { grep { /^old/ } @log }, 5);
    is_deeply \@log, ['new: PONG', 'old: nil'], 'the owed reply arrives';
    is $r->pending_count, 0, 'nothing outstanding';

    my @order;
    $r->echo($_, sub { push @order, $_[0] }) for qw(a b);
    is $r->waiting_count, 1, 'max_pending holds again on the new connection';
    run_until(sub { @order == 2 }, 5);
    is_deeply \@order, [qw(a b)], 'both sent in order';
    $r->disconnect;
}

# a reply still owed when the object goes fails with the others
{
    my @log;
    my $r = EV::Redis->new(path => $hung_path, max_pending => 1,
        on_error => sub { push @log, "error: $_[0]" });
    $r->get('never', sub { push @log, 'old: ' . ($_[1] // 'reply') });
    run_until(sub { 0 }, 0.2);
    $r->disconnect;
    $r->connect_unix($connect_info{sock});
    $r->ping(sub { push @log, 'new: ' . ($_[1] // $_[0]) });
    run_until(sub { grep { /^new/ } @log }, 5);
    undef $r;
    is_deeply \@log, ['new: PONG', 'old: disconnected'],
        'destroying the object fails the owed reply';
}

done_testing;
