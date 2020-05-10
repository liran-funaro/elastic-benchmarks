"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2019 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import os
import getpass

from cloudexp.util import xml
from cloudexp.util import shell
from cloudexp.util import network

from mom.logged_object import LoggedObject

try:
    import libvirt
except ImportError:
    libvirt = None


class LibvirtDriver(LoggedObject):
    """
    libvirtInterface provides a wrapper for the libvirt API so that libvirt-
    related error handling can be consolidated in one place.  An instance of
    this class provides a single libvirt connection that can be shared by all
    threads.  If the connection is broken, an attempt will be made to reconnect.
    """
    def __init__(self, uri):
        LoggedObject.__init__(self)
        self.conn = None
        self.uri = uri
        libvirt.registerErrorHandler(self.__libvirt_error_handler, None)
        self.__connect()

    ###########################################################################
    # Interface functions
    ###########################################################################

    def hostname(self):
        """
        :return: The system hostname on which the hypervisor is running.
            If we are connected to a remote system, then this returns the
            hostname of the remote system.
        """
        try:
            return self.conn.getHostname()
        except libvirt.libvirtError as e:
            self.libvirt_exception_handler(e)
            raise e

    def defined_domains(self):
        """
        :return: A list of the defined domains
        """
        try:
            return self.conn.listDefinedDomains()
        except libvirt.libvirtError as e:
            self.libvirt_exception_handler(e)
            raise e

    def active_domain_ids(self):
        """
        :return: List of all the active domains IDs
        """
        try:
            return self.conn.listDomainsID()
        except libvirt.libvirtError as e:
            self.libvirt_exception_handler(e)
            raise e

    def get_domain(self, domain_name):
        """
        :param domain_name: A domain name
        :return: A IDomainDriver object
        """
        dom = self.get_domain_object(dom_name=domain_name)
        if dom is None:
            raise Exception("Could not find guest named %s" % domain_name)

        return LibvirtDomainDriver(self, dom, domain_name)

    def get_domain_by_id(self, domain_id):
        """
        :param domain_id: A domain ID
        :return: A IDomainDriver object
        """
        dom = self.get_domain_object(dom_id=domain_id)
        if dom is None:
            return None

        return LibvirtDomainDriver(self, dom)

    def define_descriptor(self, xml_string):
        """
        Define a new guest using a XML descriptor
        :param xml_string: XML descriptor as string
        :return: A IDomainDriver object if successful, None otherwise
        """
        dom = self.define_descriptor_dom(xml_string)
        if dom is None:
            self.logger.error("Could not define new domain")
            return None
        return LibvirtDomainDriver(self, dom)

    def network_descriptor(self, network_name):
        """
        Get the a network XML descriptor
        :param network_name: The name of the network
        :return: An XML string
        """
        try:
            net = self.conn.networkLookupByName(network_name)
            return net.XMLDesc(0)
        except libvirt.libvirtError as e:
            self.libvirt_exception_handler(e)
            raise e

    ###########################################################################
    # Helper functions (public)
    ###########################################################################

    def define_descriptor_dom(self, xml_string):
        """
        Define/update a new guest.
        This is not private(i.e., __) because it used by the guest driver.
        :param xml_string: An XML describing the guest
        :return: A libivrt dom element if successful, None otherwise
        """
        try:
            return self.conn.defineXML(xml_string)
        except libvirt.libvirtError as e:
            self.libvirt_exception_handler(e)
            raise e

    def reconnect(self):
        """
        Attempt to reconnect to libvirt
        :return: True if successful, otherwise False
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except libvirt.libvirtError:
                pass  # The connection is in a strange state so ignore these
            finally:
                self.conn = None

        try:
            return self.__connect()
        except libvirt.libvirtError as e:
            self.logger.error("Exception while reconnecting: %s", e)
            return False

    def get_domain_object(self, dom_name=None, dom_id=None):
        """
        Get a connection to a libvirt domain object
        :param dom_name: (Optional) Domain name
        :param dom_id: (Optional) Domain ID
        :return: A libvirt domain object
        """
        if dom_id is not None:
            try:
                dom = self.conn.lookupByID(dom_id)
            except libvirt.libvirtError as e:
                self.libvirt_exception_handler(e)
            else:
                return dom

        if dom_name is not None:
            try:
                dom = self.conn.lookupByName(dom_name)
            except libvirt.libvirtError as e:
                self.libvirt_exception_handler(e)
            else:
                return dom

    def libvirt_exception_handler(self, e):
        """
        Handle exception by trying to reconnect to libvirt
        :param e: Exception object
        :return: True if reconnect was successful, otherwise False
        """
        reconnect_errors = (libvirt.VIR_ERR_SYSTEM_ERROR, libvirt.VIR_ERR_INVALID_CONN)
        do_nothing_errors = (libvirt.VIR_ERR_NO_DOMAIN,)
        error = e.get_error_code()
        if error in reconnect_errors:
            self.logger.warning('Connection lost, reconnecting.')
            return self.reconnect()
        elif error in do_nothing_errors:
            pass
        else:
            self.logger.warning('Unhandled libvirt exception (%i) %s', error, e)

    ###########################################################################
    # Helper functions (private)
    ###########################################################################

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    def __connect(self):
        try:
            self.conn = libvirt.open(self.uri)
            return True
        except libvirt.libvirtError as e:
            self.logger.exception("Error setting up connection: %s", e)
            return False

    def __libvirt_error_handler(self, ctx, error, dummy=None):
        """
        Older versions of the libvirt python bindings required an extra parameter.
        Hence 'dummy'.
        """
        pass


class LibvirtDomainDriver(LoggedObject):
    IMAGES_FOLDER = "/var/lib/libvirt/images"

    def __init__(self, libvirt_driver, domain, domain_name=None):
        self.libvirt_driver = libvirt_driver
        self.domain = domain
        if domain_name is None:
            domain_name = domain.name()
        self.domain_name = domain_name
        LoggedObject.__init__(self, domain_name)

    ###########################################################################
    # Interface functions
    ###########################################################################

    def id(self):
        """
        :return: The domain ID
        """
        try:
            return self.domain.ID()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def name(self):
        """
        :return: The domain name
        """
        try:
            return self.domain.name()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def UUID(self):
        """
        :return: The domain UUID as string
        """
        try:
            return self.domain.UUIDString()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def info(self):
        """
        :return: Extracted information about the domain as key/value (dict)
        state - The domain state defined by libvirt as:
                VIR_DOMAIN_NOSTATE  = 0 : no state
                VIR_DOMAIN_RUNNING  = 1 : the domain is running
                VIR_DOMAIN_BLOCKED  = 2 : the domain is blocked on resource
                VIR_DOMAIN_PAUSED   = 3 : the domain is paused by user
                VIR_DOMAIN_SHUTDOWN = 4 : the domain is being shut down
                VIR_DOMAIN_SHUTOFF  = 5 : the domain is shut off
                VIR_DOMAIN_CRASHED  = 6 : the domain is crashed
        max_memory - The maximum amount of memory the guest may use
        current_memory - The current memory limit (set by ballooning)
        vcpus_count - Number of virtual CPUs
        cpu_runtime -
        """
        info_state_dict = {0: "no-state", 1: "running", 2: "blocked",
                           3: "paused", 4: "shutdown", 5: "shutoff",
                           6: "crashed"}
        try:
            info = self.domain.info()
            return {
                'state': info[0],
                'state_text': info_state_dict[info[0]],
                'max_memory': info[1],
                'current_memory': info[2],
                'vcpus_count': info[3],
                'cpu_runtime': info[4]
            }
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def is_active(self):
        """
        :return: True if the domain currently active (running or suspended)
        """
        try:
            return bool(self.domain.isActive())
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def is_running(self):
        """
        :return: True if the domain currently running (active and not suspended)
        """
        try:
            state = self.domain.info()[0]
            return libvirt.VIR_DOMAIN_BLOCKED >= state >= libvirt.VIR_DOMAIN_RUNNING
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def is_suspended(self):
        """
        :return: True if the domain currently suspended (active and not running)
        """
        try:
            state = self.domain.info()[0]
            return state == libvirt.VIR_DOMAIN_PAUSED
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def memory_stats(self):
        """
        :return: Extracted memory statistics for a domain

        The following statistics may be available depending on the
            libvirt version, qemu version, and guest operation system version:
        available - The total amount of available memory (KB)
        unused    - The amount of memory that is not being used for any purpose (KB)
        anon_pages    - The amount of memory used for anonymous memory areas (KB)
        swap_in       - The amount of memory swapped in since boot (pages)
        swap_out      - The amount of memory swapped out since boot (pages)
        major_fault   - The amount of major page faults since boot (pages)
        minor_fault   - The amount of minor page faults since boot (pages)
        """
        try:
            return self.domain.memoryStats()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def set_memory(self, target):
        """
        Dynamically change the target amount of physical memory allocated to a
        domain.
        This command only changes the runtime configuration of the domain,
        so can only be called on an active domain.

        :param target: The target memory in KB
        :return: None
        """
        try:
            return self.domain.setMemory(target)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def set_max_memory(self, max_kb):
        try:
            self.domain.setMaxMemory(max_kb)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def max_memory(self):
        """
        Retrieve the maximum amount of physical memory allocated to a
        domain.
        :return: The maximum allocation in KB
        """
        try:
            return self.domain.maxMemory()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def set_cpu_count(self, cpu_count):
        try:
            return self.domain.setVcpus(cpu_count)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def pinned_vcpus(self):
        """
        Query the CPU affinity setting of all virtual CPUs of domain
        :return: A list of CPUs that this domain can use
        """
        try:
            all_vcpu_pin = self.domain.vcpuPinInfo(0)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

        if all_vcpu_pin is None:
            return

        pinned_cpus = set()
        for vcpu_pin in all_vcpu_pin:
            for cpu, pinned in enumerate(vcpu_pin):
                if pinned:
                    pinned_cpus.add(cpu)

        return sorted(pinned_cpus)

    def set_pinned_vcpus(self, cpu_list):
        """
        Pin guest to specific CPUs
        :param cpu_list: A list of CPU IDs
        :return: None
        """
        try:
            all_vcpu_pin = self.domain.vcpuPinInfo(0)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

        vcpus_count = len(all_vcpu_pin)
        cpu_vector_len = len(all_vcpu_pin[0])
        cpu_list = set(cpu_list)
        cpus_tuple = tuple(x in cpu_list for x in range(cpu_vector_len))

        self.domain.pinEmulator(cpus_tuple)
        for i in range(vcpus_count):
            self.domain.pinVcpu(i, cpus_tuple)

    def descriptor(self):
        """
        Retrieve the domain's XML descriptor
        :return: XML descriptor
        """
        try:
            return self.domain.XMLDesc(libvirt.VIR_DOMAIN_XML_UPDATE_CPU)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def update_descriptor(self, xml_string):
        """
        Updates the guest descriptor
        :param xml_string: An XML that describe the guest (must have the same name)
        :return: True if successful
        """
        guest_name = xml.get_element_text(xml_string, ".//name")
        if guest_name != self.name():
            self.logger.error("Updated descriptor must have the same guest name. Was: %s", guest_name)
            return False

        dom = self.libvirt_driver.define_descriptor_dom(xml_string)
        if dom is None:
            self.logger.error("Could not update the domain descriptor")
            return False

        self.domain = dom
        return True

    def update_max_vcpus(self, max_vcpus):
        desc_xml = xml.get_xml(self.descriptor())
        vcpu = desc_xml.find("vcpu")
        if vcpu is None:
            vcpu = desc_xml.makeelement("vcpu", {})
            desc_xml.insert(1, vcpu)

        if "current" in vcpu.attrib:
            vcpu.attrib.pop("current")

        vcpu.text = str(max_vcpus)

        self.libvirt_driver.define_descriptor_dom(xml.to_string(desc_xml))

    def undefine(self):
        """
        Undefine a domain. If the domain is running, it's converted to
        transient domain, without stopping it. If the domain is inactive,
        the domain configuration is removed.
        :return: None
        """
        try:
            return self.domain.undefine()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def start(self):
        """
        Launch a defined domain. If the call succeeds the domain moves from the
        defined to the running domains pools.  The domain will not be paused.
        :return: None
        """
        try:
            if not self.is_active():
                self.domain.create()

            if not self.is_running():
                self.domain.resume()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def start_suspended(self):
        """
        Launch a defined domain. If the call succeeds the domain moves from the
        defined to the running domains pools.  The domain will be paused.
        :return: None
        """
        try:
            if not self.is_active():
                self.domain.createWithFlags(libvirt.VIR_DOMAIN_START_PAUSED)
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def resume(self):
        """
        Resume a suspended domain, the process is restarted from the state where
        it was frozen.
        :return: None
        """
        try:
            return self.domain.resume()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def suspend(self):
        """
        Suspends an active domain, the process is frozen without further access
        to CPU resources and I/O but the memory used by the domain at the
        hypervisor level will stay allocated.
        :return: None
        """
        try:
            self.domain.suspend()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def destroy(self):
        """
        Destroy the domain. The running instance is shutdown if not down
        already and all resources used by it are given back to the hypervisor.
        This function may require privileged access
        :return: None
        """
        if not self.is_running():
            return

        try:
            self.domain.destroy()
        except libvirt.libvirtError as e:
            self.libvirt_domain_exception_handler(e)
            raise e

    def reset_image(self, master_image, enable_image_cow=True):
        """
        Reset a guest image
        :param master_image: Use this image to set the guest image initial state
        :param enable_image_cow: Use copy-on-write (COW) on the master image
        :return: None
        """
        master_image_path = os.path.join(self.IMAGES_FOLDER, master_image)
        vm_image_path = self.get_descriptor_attribute(".//disk//source", "file")

        # evil hack - TODO: remove me when bug is fixed
        shell.run(["chown", "%s:libvirtd" % getpass.getuser(), vm_image_path], as_root=True)

        if enable_image_cow:
            self.logger.info("Creating guest image (master as backing copy - COW)")
            out, err = shell.run(["qemu-img", "create", "-b", master_image_path, "-f", "qcow2", vm_image_path])
        else:
            self.logger.info("Creating guest image (full copy of master image)")
            out, err = shell.run(["cp", "-f", master_image_path, vm_image_path])

        if err:
            self.logger.error("Failed to create guest image: %s", err)

    def network_interface(self):
        """
        :return: The VM's network name
        """
        try:
            return self.get_descriptor_attribute(".//devices/interface[@type='network']/source", "network")
        except:
            return self.get_descriptor_attribute(".//devices/interface[@type='network']/target", "dev")

    def network_descriptor(self):
        """
        Get the VM's network XML descriptor
        :return: An XML string
        """
        network_name = self.network_interface()
        return self.libvirt_driver.network_descriptor(network_name)

    def mac(self):
        """ Returns the guest's MAC address """
        try:
            res = self.get_descriptor_attribute(".//devices/interface[@type='network']/mac", 'address')
        except:
            res = None

        if res is None:
            self.logger.error("MAC address not found")

        return res

    def ip(self):
        """ Returns the guest's IP address """
        mac = self.mac()
        try:
            ip = self.get_network_descriptor_attribute(".//host/[@mac='%s']" % mac, "ip")
            if ip is not None:
                return ip
        except Exception as e:
            self.logger.warning("Could not find MAC in network's DHCP: %s", e)

        try:
            ip = network.find_ip(mac)
            if ip is not None:
                return ip
        except Exception as e:
            self.logger.warning("Could not find MAC in ARP output: %s", e)

        name = self.name()
        try:
            ip = self.get_network_descriptor_attribute(".//host/[@name='%s']" % name, "ip")
            if ip is not None:
                return ip
        except Exception as e:
            self.logger.warning("Could not find guest name in network's DHCP: %s", e)

        return name

    ###########################################################################
    # Helper functions (public)
    ###########################################################################

    def get_descriptor_attribute(self, xpath_query, attr):
        """
        Find an attribute in the guest descriptor
        :param xpath_query: An xPath query
        :param attr: The attribute to find
        :return: An ElementTree attribute object
        """
        descriptor = self.descriptor()
        return xml.find_attribute(descriptor, xpath_query, attr)

    def get_network_descriptor_attribute(self, xpath_query, attr):
        """
        Find specific attribute in the VM's network descriptor
        :param xpath_query: A query to find
        :param attr: An attribute name to find
        :return: The first attribute value that matches or raise KeyError
        """
        network_descriptor = self.network_descriptor()
        return xml.find_attribute(network_descriptor, xpath_query, attr)

    def set_descriptor_fields(self, element_path, text, **attributes):
        """
        Update specific element and attributes in the guest descriptor
        :param element_path: An XML path
        :param text: The text to set at the given path
        :param attributes: The attributes to set at the given path
        :return: See update_descriptor()
        """
        updated = xml.get_updated_xml(self.descriptor(), element_path, text, **attributes)
        return self.update_descriptor(updated)

    def libvirt_domain_exception_handler(self, e):
        """
        Handle exception by trying to reconnect to libvirt
        :param e: Exception object
        :return: True if reconnect was successful, otherwise False
        """
        success = self.libvirt_driver.libvirt_exception_handler(e)
        if success:
            self.domain = self.libvirt_driver.get_domain_object(dom_name=self.domain_name)
