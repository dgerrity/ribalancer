from pprint import pprint as pp
from datetime import datetime, timedelta
from collections import defaultdict as dd
from boto import ec2 as ec2m

class IL(object):
    """
    This object represents an Instance List and it provides a set of
    handy arguments that operate on the list of instances or reserved instances
    that is contained in the list.

    This object also tries to simulate the list API, although it's not a real
    list.
    """
    def __init__(self, l=None):
        self.l = l
        if l is None:
            self.l = []

    def tags(self, tagname):
        return set(item.tags.get(tagname) for item in self.l)

    def append(self, item):
        self.l.append(item)

    def __iter__(self):
        return iter(self.l)

    def __getitem__(self, val):
        if isinstance(val, slice):
            return IL(self.l[val.start:val.stop:val.step])
        return self.l[val]

    def __len__(self):
        if self.l and hasattr(self.l[0], 'instance_count'):
            return sum(item.instance_count for item in self.l)
        return len(self.l)

    def sort(self, key=None):
        self.l.sort(key=key)

    def group_by_zone(self):
        zones = dd(lambda : IL())
        for i in self.l:
            if hasattr(i, 'placement'):
                zones[i.placement].append(i)
            elif hasattr(i, 'availability_zone'):
                zones[i.availability_zone].append(i)
            else:
                raise TypeError("unknown object type %s" % (type(i),))
        return zones

class RegionalMap(object):
    """
    This object represents the structure of a region in terms of instances,
    spots and reserved instances
    """

    def __init__(self, region):
        self.instances = dd(lambda: dd(lambda : IL()))

        ec2 = ec2m.connect_to_region(region)

        for reserved in ec2.get_all_reserved_instances():
            if reserved.state != "active":
                continue
            self.addri(reserved.instance_type, reserved.availability_zone, reserved)

        for reservation in ec2.get_all_instances():
            for instance in reservation.instances:
                if instance.state != 'running':
                    continue
                if instance.spot_instance_request_id:
                    self.adds(instance.instance_type, instance.placement, instance)
                else:
                    self.addi(instance.instance_type, instance.placement, instance)


    def addri(self, type_, zone, ri):
        self.instances[type_]["ri"].append(ri)

    def addi(self, type_, zone, ii):
        self.instances[type_]['iis'].append(ii)

    def adds(self, type_, zone, spot):
        self.instances[type_]['spot'].append(spot)

    def ideal_layout(self, age):
        """
        This is a relatively simple algorithm.

        Count how many RIs are available in a region for a size,
        then sorts instances of that size in the region by start date asc,
        then cuts the list at the number of RIs and gets the zone and count,
        and that would be the recommended layout

        For the remainder if we're running an instance for longer than a given
        period of time, it considers the instance a missing RI and suggests the
        purchase.

        The return value of the function is a dictionary of

        instance_size -> zone -> [net change, new suggested reservations]

        if the total result of a region is a negative net change it means that
        there's too many reserved instances.
        """
        changes = dd(lambda: dd(lambda : []))
        for type_, counts in self.instances.iteritems():
            total_ris = len(counts["ri"])

            # deserving_instances are all instances that have priority in being
            # covered by a reserved instance.
            counts['iis'].sort(lambda i: i.launch_time)
            deserving_instances = counts['iis'][:total_ris]

            # And here we begin to find coverage holes.
            grouped_instances = deserving_instances.group_by_zone()
            grouped_ris = counts["ri"].group_by_zone()

            for zone in set(grouped_instances.keys() +
                            grouped_ris.keys()):
                instances = grouped_instances[zone]
                ris = grouped_ris[zone]

                num_instances = len(instances)
                num_ris = len(ris)
                if num_ris == num_instances:
                    continue

                changes[type_][zone].append(num_instances-num_ris)

            rest = counts['iis'][total_ris:]
            if not rest:
                continue

            age_string = age.isoformat().rsplit('.')[0] + ".000Z"
            new_suggested = [i for i in rest if i.launch_time <= age_string]

            if not new_suggested:
                continue

            changes[type_][zone].append("+R:%s" % len(new_suggested))


        return changes

if __name__ == "__main__":
    res_age = datetime.utcnow() - timedelta(weeks=1)
    for region in ec2m.RegionData:
        if region in ["cn-north-1", "us-gov-west-1"]:
            continue

        print "=============  " + region + "  =============="
        rrimap = RegionalMap(region)
        pp({k: dict(v) for k, v in dict(rrimap.ideal_layout(res_age)).items()})

