// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
/// Kiosk testing strategy:
/// - [ ] test purchase flow
/// - [ ] test purchase cap flow
/// - [ ] test withdraw methods
module sui::kiosk_tests {
    use sui::kiosk_test_utils::{Self as test, Asset};
    use sui::transfer_policy as policy;
    use sui::sui::SUI;
    use sui::kiosk;
    use sui::coin;

    const AMT: u64 = 10_000;

    #[test]
    fun test_place_and_take() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, policy_cap) = test::get_policy(ctx);

        kiosk::place(&mut kiosk, &owner_cap, asset);

        assert!(kiosk::has_item(&kiosk, item_id), 0);
        let asset = kiosk::take(&mut kiosk, &owner_cap, item_id);
        assert!(!kiosk::has_item(&kiosk, item_id), 0);

        test::return_policy(policy, policy_cap, ctx);
        test::return_kiosk(kiosk, owner_cap, ctx);
        test::return_assets(vector[ asset ]);
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::EItemLocked)]
    fun test_taking_not_allowed() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, _policy_cap) = test::get_policy(ctx);

        kiosk::lock(&mut kiosk, &owner_cap, &policy, asset);
        let _asset = kiosk::take<Asset>(&mut kiosk, &owner_cap, item_id);
        abort 1337
    }

    #[test]
    fun test_purchase() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, policy_cap) = test::get_policy(ctx);

        kiosk::place_and_list(&mut kiosk, &owner_cap, asset, AMT);
        assert!(kiosk::is_listed(&kiosk, item_id), 0);
        let payment = coin::mint_for_testing<SUI>(AMT, ctx);
        let (asset, request) = kiosk::purchase(&mut kiosk, item_id, payment);
        assert!(!kiosk::is_listed(&kiosk, item_id), 0);
        policy::confirm_request(&mut policy, request);

        test::return_kiosk(kiosk, owner_cap, ctx);
        test::return_assets(vector[ asset ]);
        test::return_policy(policy, policy_cap, ctx);
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::EIncorrectAmount)]
    fun test_purchase_wrong_amount() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, _policy_cap) = test::get_policy(ctx);

        kiosk::place_and_list(&mut kiosk, &owner_cap, asset, AMT);
        let payment = coin::mint_for_testing<SUI>(AMT + 1, ctx);
        let (_asset, request) = kiosk::purchase(&mut kiosk, item_id, payment);
        policy::confirm_request(&mut policy, request);

        abort 1337
    }

    #[test]
    fun test_purchase_cap() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, policy_cap) = test::get_policy(ctx);

        kiosk::place(&mut kiosk, &owner_cap, asset);
        let purchase_cap = kiosk::list_with_purchase_cap(&mut kiosk, &owner_cap, item_id, AMT, ctx);
        let payment = coin::mint_for_testing<SUI>(AMT, ctx);
        assert!(kiosk::is_listed_exclusively(&kiosk, item_id), 0);
        let (asset, request) = kiosk::purchase_with_cap(&mut kiosk, purchase_cap, payment);
        assert!(!kiosk::is_listed_exclusively(&kiosk, item_id), 0);
        policy::confirm_request(&mut policy, request);

        test::return_kiosk(kiosk, owner_cap, ctx);
        test::return_assets(vector[ asset ]);
        test::return_policy(policy, policy_cap, ctx);
    }

    #[test]
    fun test_purchase_cap_return() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let (policy, policy_cap) = test::get_policy(ctx);

        kiosk::place(&mut kiosk, &owner_cap, asset);
        let purchase_cap = kiosk::list_with_purchase_cap<test::Asset>(&mut kiosk, &owner_cap, item_id, AMT, ctx);
        kiosk::return_purchase_cap(&mut kiosk, purchase_cap);
        let asset = kiosk::take(&mut kiosk, &owner_cap, item_id);

        test::return_kiosk(kiosk, owner_cap, ctx);
        test::return_assets(vector[ asset ]);
        test::return_policy(policy, policy_cap, ctx);
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::EAlreadyListed)]
    fun test_purchase_cap_already_listed_fail() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);

        kiosk::place_and_list(&mut kiosk, &owner_cap, asset, AMT);
        let _purchase_cap = kiosk::list_with_purchase_cap<test::Asset>(&mut kiosk, &owner_cap, item_id, AMT, ctx);

        abort 1337
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::EListedExclusively)]
    fun test_purchase_cap_issued_list_fail() {
        let ctx = &mut test::ctx();
        let (asset, item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);

        kiosk::place(&mut kiosk, &owner_cap, asset);
        let purchase_cap = kiosk::list_with_purchase_cap<test::Asset>(&mut kiosk, &owner_cap, item_id, AMT, ctx);
        kiosk::list<test::Asset>(&mut kiosk, &owner_cap, item_id, AMT);
        kiosk::return_purchase_cap(&mut kiosk, purchase_cap);

        abort 1337
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::ENotEmpty)]
    fun test_kiosk_has_items() {
        let ctx = &mut test::ctx();
        let (_policy, _cap) = test::get_policy(ctx);
        let (asset, _item_id) = test::get_asset(ctx);
        let (kiosk, owner_cap) = test::get_kiosk(ctx);

        kiosk::place(&mut kiosk, &owner_cap, asset);
        test::return_kiosk(kiosk, owner_cap, ctx);

        abort 1337
    }

    #[test]
    fun test_withdraw_default() {
        let ctx = &mut test::ctx();
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let profits = kiosk::withdraw(&mut kiosk, &owner_cap, std::option::none(), ctx);

        coin::burn_for_testing(profits);
        test::return_kiosk(kiosk, owner_cap, ctx);
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::ENotEnough)]
    fun test_withdraw_more_than_there_is() {
        let ctx = &mut test::ctx();
        let (kiosk, owner_cap) = test::get_kiosk(ctx);
        let _profits = kiosk::withdraw(&mut kiosk, &owner_cap, std::option::some(100), ctx);

        abort 1337
    }

    #[test]
    fun test_disallow_extensions_access_as_owner() {
        let ctx = &mut test::ctx();
        let (kiosk, owner_cap) = test::get_kiosk(ctx);

        kiosk::set_allow_extensions(&mut kiosk, &owner_cap, false);
        let _uid_mut = kiosk::uid_mut_as_owner(&mut kiosk, &owner_cap);
        test::return_kiosk(kiosk, owner_cap, ctx);
    }

    #[test]
    #[expected_failure(abort_code = sui::kiosk::EExtensionsDisabled)]
    fun test_disallow_extensions() {
        let ctx = &mut test::ctx();
        let (kiosk, owner_cap) = test::get_kiosk(ctx);

        kiosk::set_allow_extensions(&mut kiosk, &owner_cap, false);
        let _ = kiosk::uid_mut(&mut kiosk);

        abort 1337
    }
}
