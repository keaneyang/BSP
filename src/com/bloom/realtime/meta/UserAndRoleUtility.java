package com.bloom.runtime.meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.bloom.security.ObjectPermission;
import com.bloom.security.ObjectPermission.PermissionType;

public class UserAndRoleUtility
{
  public boolean isDisallowedPermission(ObjectPermission perm)
  {
    if (perm.getType().equals(ObjectPermission.PermissionType.disallow)) {
      return true;
    }
    return false;
  }
  
  public List<ObjectPermission> getDisallowedPermissionFromList(List<ObjectPermission> perms)
  {
    List<ObjectPermission> list = new ArrayList();
    if ((perms == null) || (perms.isEmpty())) {
      return list;
    }
    for (ObjectPermission perm : perms) {
      if (isDisallowedPermission(perm)) {
        list.add(new ObjectPermission(perm));
      }
    }
    return list;
  }
  
  public boolean doesListHasImpliedPerm(List<ObjectPermission> list, ObjectPermission inputPerm)
  {
    if ((list == null) || (list.isEmpty())) {
      return false;
    }
    if (inputPerm == null) {
      return false;
    }
    for (ObjectPermission perm : list) {
      if (perm.implies(inputPerm)) {
        return true;
      }
    }
    return false;
  }
  
  public boolean doesListHasExactPerm(List<ObjectPermission> list, ObjectPermission inputPerm)
  {
    if ((list == null) || (list.isEmpty())) {
      return false;
    }
    if (inputPerm == null) {
      return false;
    }
    for (ObjectPermission perm : list) {
      if (perm.equals(inputPerm)) {
        return true;
      }
    }
    return false;
  }
  
  public List<ObjectPermission> getDependentPerms(ObjectPermission permToBeRevoked, List<ObjectPermission> permList)
  {
    List<ObjectPermission> list1 = getDisallowedPermissionFromList(permList);
    
    List<ObjectPermission> list2 = new ArrayList();
    
    for (ObjectPermission disAllowedPerm : list1)
    {
      ObjectPermission reverseDisAllowedPerm = new ObjectPermission(disAllowedPerm, ObjectPermission.PermissionType.allow);
      if (permToBeRevoked.implies(reverseDisAllowedPerm)) {
        list2.add(new ObjectPermission(disAllowedPerm));
      }
    }
    List<ObjectPermission> list3 = new ArrayList();
    for (Iterator i$ = list2.iterator(); i$.hasNext();)
    {
    	ObjectPermission disAllowedPerm = (ObjectPermission)i$.next();
    	ObjectPermission revDisAllowedPerm = new ObjectPermission(disAllowedPerm, ObjectPermission.PermissionType.allow);
      for (ObjectPermission perm : permList) {
        if (perm.implies(revDisAllowedPerm)) {
          list3.add(new ObjectPermission(disAllowedPerm));
        }
      }
    }
    
    Set<ObjectPermission> set2 = new HashSet(list2);
    Set<ObjectPermission> set3 = new HashSet(list3);
    set2.removeAll(set3);
    List<ObjectPermission> result = new ArrayList();
    result.addAll(set2);
    return result;
  }
}
